package framework

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/GoogleCloudPlatform/gke-enterprise-mt/pkg/mtmetrics"
	"github.com/prometheus/client_golang/prometheus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	providerconfig "k8s.io/ingress-gce/pkg/apis/providerconfig/v1"
	"k8s.io/ingress-gce/pkg/flags"
	"k8s.io/ingress-gce/pkg/multiproject/common/finalizer"
	providerconfigclient "k8s.io/ingress-gce/pkg/providerconfig/client/clientset/versioned"
	"k8s.io/klog/v2"
)

var tenantFactories sync.Map

// GetOrCreateFactory returns the shared MTMetricFactory for the given tenantUID,
// creating and registering it with DefaultMultiGatherer if it does not yet exist.
func GetOrCreateFactory(tenantUID string) *mtmetrics.MTMetricFactory {
	if v, ok := tenantFactories.Load(tenantUID); ok {
		return v.(*mtmetrics.MTMetricFactory)
	}

	f := mtmetrics.NewMTMetricFactory(tenantUID, prometheus.DefaultRegisterer, mtmetrics.DefaultGlobalTracker)
	actual, loaded := tenantFactories.LoadOrStore(tenantUID, f)
	if loaded {
		f.Cleanup()
		return actual.(*mtmetrics.MTMetricFactory)
	}
	mtmetrics.DefaultMultiGatherer.Unregister(tenantUID)
	_ = mtmetrics.DefaultMultiGatherer.Register(tenantUID, f.Registry())
	return f
}

// RegisterTenantFactory associates an existing MTMetricFactory with tenantUID in the
// shared registry and registers its prometheus.Registry with DefaultMultiGatherer.
func RegisterTenantFactory(tenantUID string, f *mtmetrics.MTMetricFactory) {
	if f == nil {
		return
	}
	mtmetrics.DefaultMultiGatherer.Unregister(tenantUID)
	_ = mtmetrics.DefaultMultiGatherer.Register(tenantUID, f.Registry())
	tenantFactories.Store(tenantUID, f)
}

// CleanupTenant unregisters the tenant from DefaultMultiGatherer, resets its
// GlobalMetricsTracker state, and removes the cached MTMetricFactory.
func CleanupTenant(tenantUID string) {
	v, ok := tenantFactories.LoadAndDelete(tenantUID)
	if ok {
		v.(*mtmetrics.MTMetricFactory).Cleanup()
	} else {
		mtmetrics.DefaultGlobalTracker.ResetTenant(tenantUID)
	}
	mtmetrics.DefaultMultiGatherer.Unregister(tenantUID)
}

type deletingTenantState struct {
	tenantUID string
	cancel    context.CancelFunc
}

// manager coordinates lifecycle of controllers scoped to individual ProviderConfigs.
// It ensures per-ProviderConfig controller startup is idempotent, adds/removes
// finalizers, and wires stop channels for clean shutdown.
//
// This manager assumes it is invoked by a workqueue that guarantees
// the same ProviderConfig key is never processed concurrently.
type manager struct {
	controllers *ControllerMap

	logger               klog.Logger
	providerConfigClient providerconfigclient.Interface
	finalizerName        string
	controllerStarter    ControllerStarter

	deletingMu   sync.Mutex
	deleting     map[string]*deletingTenantState
	tenantUIDs   map[string]string
	failedStarts map[string]bool
}

// newManager constructs a new generic ProviderConfig controller manager.
// It does not start any controllers until StartControllersForProviderConfig is invoked.
func newManager(
	providerConfigClient providerconfigclient.Interface,
	finalizerName string,
	controllerStarter ControllerStarter,
	logger klog.Logger,
) *manager {
	return &manager{
		controllers:          NewControllerMap(),
		logger:               logger,
		providerConfigClient: providerConfigClient,
		finalizerName:        finalizerName,
		controllerStarter:    controllerStarter,
		deleting:             make(map[string]*deletingTenantState),
		tenantUIDs:           make(map[string]string),
		failedStarts:         make(map[string]bool),
	}
}

// providerConfigKey returns the key for a ProviderConfig in the controller map.
func providerConfigKey(pc *providerconfig.ProviderConfig) string {
	return pc.Name
}

func resolveTenantUID(pc *providerconfig.ProviderConfig, logger klog.Logger) string {
	tenantUID := ""
	if pc.Spec.PrincipalInfo != nil {
		tenantUID = pc.Spec.PrincipalInfo.ID
	}
	if tenantUID == "" {
		logger.Info("tenant UID from Spec.PrincipalInfo.ID is empty, fallback to pc.Name")
		tenantUID = pc.Name
	}
	return tenantUID
}

func controllerNameFromFinalizer(finalizerName string) string {
	name := finalizerName
	if idx := strings.LastIndex(name, "/"); idx != -1 && idx+1 < len(name) {
		name = name[idx+1:]
	}
	name = strings.TrimSuffix(name, "-cleanup")
	name = strings.TrimPrefix(name, "providerconfig-")
	if name == "" {
		return finalizerName
	}
	return name
}

func (m *manager) setControllerRunningMetric(tenantUID string, val float64) {
	if tenantUID == "" || !flags.F.EnableMultiTenantMetrics {
		return
	}
	factory := GetOrCreateFactory(tenantUID)
	gauge, err := factory.NewGaugeVec(prometheus.GaugeOpts{
		Name: "providerconfig_controller_running",
		Help: "Indicates whether the ProviderConfig controller is running (1) or failed to start (0) for a tenant.",
	}, []string{"controller"})
	if err != nil {
		var are prometheus.AlreadyRegisteredError
		if errors.As(err, &are) {
			if existing, ok := are.ExistingCollector.(*prometheus.GaugeVec); ok {
				gauge = existing
				err = nil
			}
		}
		if err != nil {
			m.logger.Error(err, "failed to register providerconfig_controller_running gauge", "tenantUID", tenantUID)
			return
		}
	}
	gauge.WithLabelValues(controllerNameFromFinalizer(m.finalizerName)).Set(val)
}

// rollbackFinalizerOnStartFailure removes the finalizer after a start failure
// so that ProviderConfig deletion is not blocked.
func (m *manager) rollbackFinalizerOnStartFailure(pc *providerconfig.ProviderConfig, logger klog.Logger, cause error) {
	pcLatest, err := m.providerConfigClient.CloudV1().ProviderConfigs().Get(context.Background(), pc.Name, metav1.GetOptions{})
	if err != nil {
		logger.Error(err, "failed to get latest ProviderConfig for finalizer rollback", "originalError", cause)
		return
	}
	if err := finalizer.DeleteProviderConfigFinalizer(pcLatest, m.finalizerName, m.providerConfigClient, logger); err != nil {
		logger.Error(err, "failed to clean up finalizer after start failure", "originalError", cause)
	}
}

// StartControllersForProviderConfig ensures finalizers are present and starts
// the controllers associated with the given ProviderConfig. The call is
// idempotent: repeated calls for the same ProviderConfig will only start
// controllers once.
func (m *manager) StartControllersForProviderConfig(pc *providerconfig.ProviderConfig) error {
	logger := m.logger.WithValues("providerConfigName", pc.Name)

	if pc.DeletionTimestamp != nil {
		logger.V(3).Info("ProviderConfig is terminating; skipping start")
		return nil
	}

	pcKey := providerConfigKey(pc)
	tenantUID := resolveTenantUID(pc, logger)

	m.deletingMu.Lock()
	if state, exists := m.deleting[pcKey]; exists {
		state.cancel()
		delete(m.deleting, pcKey)
	}
	m.tenantUIDs[pcKey] = tenantUID
	wasFailedStart := m.failedStarts[pcKey]
	delete(m.failedStarts, pcKey)
	m.deletingMu.Unlock()

	cs, existed := m.controllers.GetOrCreate(pcKey)
	if existed && cs.stopCh != nil {
		logger.Info("Controllers for provider config already exist, skipping start")
		return nil
	}

	// If a previous start attempt failed, reset any stale factory from that failed attempt
	// before invoking controllerStarter so that controllers creating a fresh MTMetricFactory
	// do not collide with the failed attempt's factory.
	if wasFailedStart {
		CleanupTenant(tenantUID)
	}

	logger.Info("Starting controllers for provider config")

	hadFinalizer := finalizer.HasGivenFinalizer(pc.ObjectMeta, m.finalizerName)

	err := finalizer.EnsureProviderConfigFinalizer(pc, m.finalizerName, m.providerConfigClient, logger)
	if err != nil {
		if !existed {
			m.controllers.Delete(pcKey)
		}
		m.deletingMu.Lock()
		m.failedStarts[pcKey] = true
		m.deletingMu.Unlock()
		m.setControllerRunningMetric(tenantUID, 0)
		return fmt.Errorf("failed to ensure finalizer %s for provider config %s: %w", m.finalizerName, pcKey, err)
	}

	controllerStopCh, err := m.controllerStarter.StartController(pc)
	if err != nil {
		if !existed {
			m.controllers.Delete(pcKey)
		}
		if !hadFinalizer {
			m.rollbackFinalizerOnStartFailure(pc, logger, err)
		}
		m.deletingMu.Lock()
		m.failedStarts[pcKey] = true
		m.deletingMu.Unlock()
		m.setControllerRunningMetric(tenantUID, 0)
		return fmt.Errorf("failed to start controller for provider config %s: %w", pcKey, err)
	}
	if controllerStopCh == nil {
		if !existed {
			m.controllers.Delete(pcKey)
		}
		if !hadFinalizer {
			m.rollbackFinalizerOnStartFailure(pc, logger, fmt.Errorf("StartController returned nil stop channel"))
		}
		m.deletingMu.Lock()
		m.failedStarts[pcKey] = true
		m.deletingMu.Unlock()
		m.setControllerRunningMetric(tenantUID, 0)
		return fmt.Errorf("controller starter returned nil stop channel for provider config %s", pcKey)
	}

	cs.stopCh = controllerStopCh
	m.setControllerRunningMetric(tenantUID, 1)

	logger.Info("Started controllers for provider config")
	return nil
}

// StopControllersForProviderConfig stops the controllers for the given ProviderConfig
// and removes the associated finalizer. Finalizer removal is attempted even if no
// controller mapping exists, ensuring deletion can proceed after process restarts
// or when controllers were previously stopped.
func (m *manager) StopControllersForProviderConfig(pc *providerconfig.ProviderConfig) error {
	logger := m.logger.WithValues("providerConfigName", pc.Name)

	pcKey := providerConfigKey(pc)
	tenantUID := resolveTenantUID(pc, logger)

	m.deletingMu.Lock()
	m.tenantUIDs[pcKey] = tenantUID
	if _, exists := m.deleting[pcKey]; !exists {
		tCtx, cancel := context.WithCancel(context.Background())
		m.deleting[pcKey] = &deletingTenantState{
			tenantUID: tenantUID,
			cancel:    cancel,
		}
		go func(key string, c context.Context) {
			select {
			case <-time.After(24 * time.Hour):
				logger.Info("forcing cleanup for tenant after 24 hours of inactivity", "key", key)
				m.ForceCleanupTenant(key)
			case <-c.Done():
				// Cancelled when finalizer is successfully removed or cleanup forced.
			}
		}(pcKey, tCtx)
	}
	m.deletingMu.Unlock()

	if cs, exists := m.controllers.Get(pcKey); exists {
		m.controllers.Delete(pcKey)
		if cs.stopCh != nil {
			close(cs.stopCh)
			logger.Info("Signaled controller stop")
		} else {
			logger.Info("Controllers for provider config already stopped")
		}
	} else {
		logger.Info("Controllers for provider config do not exist")
	}

	// Fetch the latest ProviderConfig to ensure we have current finalizer state.
	latestPC, err := m.providerConfigClient.CloudV1().ProviderConfigs().Get(context.Background(), pc.Name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("ProviderConfig not found while stopping controllers; skipping finalizer removal")
			return nil
		}
		return fmt.Errorf("failed to get latest ProviderConfig for finalizer removal: %w", err)
	}

	err = finalizer.DeleteProviderConfigFinalizer(latestPC, m.finalizerName, m.providerConfigClient, logger)
	if err != nil {
		return fmt.Errorf("failed to delete finalizer %s for provider config %s: %w", m.finalizerName, pcKey, err)
	}
	logger.Info("Stopped controllers for provider config")
	return nil
}

// ForceCleanupTenant cancels the deletion timer and forces metrics unregistration.
func (m *manager) ForceCleanupTenant(pcKey string) {
	m.deletingMu.Lock()
	var tenantUID string
	if state, exists := m.deleting[pcKey]; exists {
		state.cancel()
		tenantUID = state.tenantUID
		delete(m.deleting, pcKey)
	} else if cachedUID, ok := m.tenantUIDs[pcKey]; ok {
		tenantUID = cachedUID
	}
	delete(m.tenantUIDs, pcKey)
	delete(m.failedStarts, pcKey)
	m.deletingMu.Unlock()

	if tenantUID == "" {
		tenantUID = pcKey
	}
	CleanupTenant(tenantUID)
}
