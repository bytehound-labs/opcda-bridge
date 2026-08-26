use std::path::Path;
#[cfg(target_os = "windows")]
use std::path::PathBuf;
use std::sync::Arc;
#[cfg(target_os = "windows")]
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::opc::MAX_NATIVE_INVENTORY_BATCH_SIZE;

/// The reason an inventory controller is not admitting another native slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PauseReason {
    Foreground,
    OpcHealth,
    HostCpu,
    Memory,
    Disk,
    Database,
    Operator,
    Circuit,
    Maintenance,
}

impl PauseReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Foreground => "foreground",
            Self::OpcHealth => "opc_health",
            Self::HostCpu => "host_cpu",
            Self::Memory => "memory",
            Self::Disk => "disk",
            Self::Database => "database",
            Self::Operator => "operator",
            Self::Circuit => "circuit",
            Self::Maintenance => "maintenance",
        }
    }
}

/// The adaptive controller's current operating state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControllerState {
    Ramping,
    Steady,
    Throttled,
    Paused(PauseReason),
}

/// The effective limits sent to the inventory worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InventoryLimits {
    pub item_rate_per_second: u32,
    pub batch_size: u32,
    pub duty_cycle_percent: u8,
}

/// Configurable floors, ceilings, and recovery timing for AIMD control.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControllerConfig {
    pub floor: InventoryLimits,
    pub canary: InventoryLimits,
    pub ceiling: InventoryLimits,
    pub unlimited_item_rate: bool,
    pub healthy_window: Duration,
    pub recovery_delay: Duration,
    pub maximum_recovery_delay: Duration,
    pub foreground_latency_absolute_ms: u64,
}

impl ControllerConfig {
    pub fn normalized(self) -> Self {
        let floor = InventoryLimits {
            item_rate_per_second: if self.unlimited_item_rate {
                0
            } else {
                self.floor.item_rate_per_second.max(1)
            },
            batch_size: self
                .floor
                .batch_size
                .clamp(1, MAX_NATIVE_INVENTORY_BATCH_SIZE),
            duty_cycle_percent: self.floor.duty_cycle_percent.clamp(1, 100),
        };
        let ceiling = InventoryLimits {
            item_rate_per_second: if self.unlimited_item_rate {
                0
            } else {
                self.ceiling
                    .item_rate_per_second
                    .max(floor.item_rate_per_second)
            },
            batch_size: self
                .ceiling
                .batch_size
                .clamp(floor.batch_size, MAX_NATIVE_INVENTORY_BATCH_SIZE),
            duty_cycle_percent: self
                .ceiling
                .duty_cycle_percent
                .clamp(floor.duty_cycle_percent, 100),
        };
        let canary = InventoryLimits {
            item_rate_per_second: if self.unlimited_item_rate {
                0
            } else {
                self.canary
                    .item_rate_per_second
                    .clamp(floor.item_rate_per_second, ceiling.item_rate_per_second)
            },
            batch_size: self.canary.batch_size.clamp(
                floor.batch_size,
                ceiling.batch_size.min(MAX_NATIVE_INVENTORY_BATCH_SIZE),
            ),
            duty_cycle_percent: self
                .canary
                .duty_cycle_percent
                .clamp(floor.duty_cycle_percent, ceiling.duty_cycle_percent),
        };
        Self {
            floor,
            canary,
            ceiling,
            unlimited_item_rate: self.unlimited_item_rate,
            healthy_window: self.healthy_window.max(Duration::from_millis(1)),
            recovery_delay: self.recovery_delay.max(Duration::from_millis(1)),
            maximum_recovery_delay: self
                .maximum_recovery_delay
                .max(self.recovery_delay.max(Duration::from_millis(1))),
            foreground_latency_absolute_ms: self.foreground_latency_absolute_ms.max(1),
        }
    }
}

impl Default for ControllerConfig {
    fn default() -> Self {
        Self {
            floor: InventoryLimits {
                item_rate_per_second: 10,
                batch_size: 1,
                duty_cycle_percent: 1,
            },
            canary: InventoryLimits {
                item_rate_per_second: 50,
                batch_size: 25,
                duty_cycle_percent: 5,
            },
            ceiling: InventoryLimits {
                item_rate_per_second: 250,
                batch_size: 100,
                duty_cycle_percent: 20,
            },
            unlimited_item_rate: false,
            healthy_window: Duration::from_secs(30),
            recovery_delay: Duration::from_secs(30),
            maximum_recovery_delay: Duration::from_secs(300),
            foreground_latency_absolute_ms: 2_000,
        }
    }
}

/// A snapshot of foreground, OPC, host, and storage health.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct ControllerObservation {
    pub foreground_active: bool,
    pub foreground_error: bool,
    pub foreground_bad_quality: bool,
    pub foreground_latency_ms: Option<u64>,
    pub baseline_latency_ms: Option<u64>,
    pub inventory_error: bool,
    pub host_cpu_percent: Option<f64>,
    pub available_memory_percent: Option<f64>,
    pub disk_active_percent: Option<f64>,
    pub disk_queue: Option<f64>,
    pub database_commit_p95_ms: Option<u64>,
    pub insufficient_disk_space: bool,
}

/// Host resource measurements used by the adaptive controller.
///
/// `None` is explicit unavailability, not a zero reading.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct HostMetrics {
    pub cpu_percent: Option<f64>,
    pub available_memory_percent: Option<f64>,
    pub disk_active_percent: Option<f64>,
    pub disk_queue: Option<f64>,
    pub process_working_set_bytes: Option<u64>,
    pub process_private_bytes: Option<u64>,
    pub process_read_bytes_per_second: Option<u64>,
    pub process_write_bytes_per_second: Option<u64>,
    pub disk_free_bytes: Option<u64>,
}

pub trait HostMetricsProvider: Send + Sync {
    fn snapshot(&self) -> HostMetrics;
}

#[derive(Debug, Default)]
pub struct UnavailableHostMetrics;

impl HostMetricsProvider for UnavailableHostMetrics {
    fn snapshot(&self) -> HostMetrics {
        HostMetrics::default()
    }
}

/// Construct the platform provider used by the gateway.
///
/// Windows exposes the process and host counters needed by the index guardrails.
/// Other platforms retain explicit `None` values until a native provider exists.
pub fn default_host_metrics_provider(index_path: &Path) -> Arc<dyn HostMetricsProvider> {
    #[cfg(target_os = "windows")]
    {
        Arc::new(WindowsHostMetrics::new(index_path))
    }
    #[cfg(not(target_os = "windows"))]
    {
        let _ = index_path;
        Arc::new(UnavailableHostMetrics)
    }
}

#[cfg(target_os = "windows")]
#[derive(Debug)]
struct WindowsHostMetrics {
    index_path: PathBuf,
    state: Mutex<WindowsMetricsState>,
}

#[cfg(target_os = "windows")]
#[derive(Debug, Default)]
struct WindowsMetricsState {
    sampled_at: Option<Instant>,
    system: Option<SystemCounters>,
    process: Option<ProcessCounters>,
}

#[cfg(target_os = "windows")]
#[derive(Debug, Clone, Copy)]
struct SystemCounters {
    busy_ticks: u64,
    total_ticks: u64,
}

#[cfg(target_os = "windows")]
#[derive(Debug, Clone, Copy)]
struct ProcessCounters {
    read_bytes: u64,
    write_bytes: u64,
}

#[cfg(target_os = "windows")]
impl WindowsHostMetrics {
    fn new(index_path: &Path) -> Self {
        Self {
            index_path: index_path.to_path_buf(),
            state: Mutex::new(WindowsMetricsState::default()),
        }
    }

    fn collect_system() -> Option<SystemCounters> {
        use windows_sys::Win32::Foundation::FILETIME;
        use windows_sys::Win32::System::Threading::GetSystemTimes;

        let mut idle = FILETIME::default();
        let mut kernel = FILETIME::default();
        let mut user = FILETIME::default();
        // The Windows metrics API has no safe Rust binding.
        // nosemgrep: rust.lang.security.unsafe-usage.unsafe-usage
        let success = unsafe { GetSystemTimes(&mut idle, &mut kernel, &mut user) };
        if success == 0 {
            return None;
        }
        let idle_ticks = filetime_ticks(idle);
        let kernel_ticks = filetime_ticks(kernel);
        let user_ticks = filetime_ticks(user);
        let total_ticks = kernel_ticks.saturating_add(user_ticks);
        Some(SystemCounters {
            busy_ticks: total_ticks.saturating_sub(idle_ticks),
            total_ticks,
        })
    }

    fn collect_process() -> (Option<u64>, Option<u64>, Option<ProcessCounters>) {
        use std::mem::size_of;
        use windows_sys::Win32::System::ProcessStatus::{
            GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS, PROCESS_MEMORY_COUNTERS_EX,
        };
        use windows_sys::Win32::System::Threading::{
            GetCurrentProcess, GetProcessIoCounters, IO_COUNTERS,
        };

        // The Windows metrics API has no safe Rust binding.
        // nosemgrep: rust.lang.security.unsafe-usage.unsafe-usage
        let process = unsafe { GetCurrentProcess() };
        let mut memory = PROCESS_MEMORY_COUNTERS_EX {
            cb: size_of::<PROCESS_MEMORY_COUNTERS_EX>() as u32,
            ..PROCESS_MEMORY_COUNTERS_EX::default()
        };
        // The Windows metrics API has no safe Rust binding.
        // nosemgrep: rust.lang.security.unsafe-usage.unsafe-usage
        let memory_ok = unsafe {
            GetProcessMemoryInfo(
                process,
                (&mut memory as *mut PROCESS_MEMORY_COUNTERS_EX).cast::<PROCESS_MEMORY_COUNTERS>(),
                memory.cb,
            ) != 0
        };

        let mut io = IO_COUNTERS::default();
        // The Windows metrics API has no safe Rust binding.
        // nosemgrep: rust.lang.security.unsafe-usage.unsafe-usage
        let io_ok = unsafe { GetProcessIoCounters(process, &mut io) != 0 };
        (
            memory_ok.then_some(memory.WorkingSetSize as u64),
            memory_ok.then_some(memory.PrivateUsage as u64),
            io_ok.then_some(ProcessCounters {
                read_bytes: io.ReadTransferCount,
                write_bytes: io.WriteTransferCount,
            }),
        )
    }

    fn collect_memory() -> Option<f64> {
        use std::mem::size_of;
        use windows_sys::Win32::System::SystemInformation::{GlobalMemoryStatusEx, MEMORYSTATUSEX};

        let mut memory = MEMORYSTATUSEX {
            dwLength: size_of::<MEMORYSTATUSEX>() as u32,
            ..MEMORYSTATUSEX::default()
        };
        // The Windows metrics API has no safe Rust binding.
        // nosemgrep: rust.lang.security.unsafe-usage.unsafe-usage
        let success = unsafe { GlobalMemoryStatusEx(&mut memory) };
        if success == 0 || memory.ullTotalPhys == 0 {
            return None;
        }
        Some(memory.ullAvailPhys as f64 * 100.0 / memory.ullTotalPhys as f64)
    }

    fn collect_disk_free(&self) -> Option<u64> {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceExW;

        let directory = self
            .index_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let mut wide = directory.as_os_str().encode_wide().collect::<Vec<_>>();
        wide.push(0);
        let mut available = 0_u64;
        // The Windows metrics API has no safe Rust binding.
        // nosemgrep: rust.lang.security.unsafe-usage.unsafe-usage
        let success = unsafe {
            GetDiskFreeSpaceExW(
                wide.as_ptr(),
                &mut available,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        (success != 0).then_some(available)
    }
}

#[cfg(target_os = "windows")]
impl HostMetricsProvider for WindowsHostMetrics {
    fn snapshot(&self) -> HostMetrics {
        let now = Instant::now();
        let system = Self::collect_system();
        let (working_set, private_bytes, process) = Self::collect_process();
        let available_memory_percent = Self::collect_memory();
        let disk_free_bytes = self.collect_disk_free();

        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => {
                return HostMetrics {
                    available_memory_percent,
                    process_working_set_bytes: working_set,
                    process_private_bytes: private_bytes,
                    disk_free_bytes,
                    ..HostMetrics::default()
                };
            }
        };
        let elapsed = state
            .sampled_at
            .map(|sampled_at| now.saturating_duration_since(sampled_at));
        let cpu_percent = state.system.zip(system).and_then(|(previous, current)| {
            let total = current.total_ticks.saturating_sub(previous.total_ticks);
            let busy = current.busy_ticks.saturating_sub(previous.busy_ticks);
            (total > 0).then_some(busy as f64 * 100.0 / total as f64)
        });
        let (process_read_bytes_per_second, process_write_bytes_per_second) = state
            .process
            .zip(process)
            .zip(elapsed)
            .and_then(|((previous, current), elapsed)| {
                let seconds = elapsed.as_secs_f64();
                (seconds > 0.0).then_some((
                    (current.read_bytes.saturating_sub(previous.read_bytes) as f64 / seconds)
                        as u64,
                    (current.write_bytes.saturating_sub(previous.write_bytes) as f64 / seconds)
                        as u64,
                ))
            })
            .map_or((None, None), |(read, write)| (Some(read), Some(write)));
        state.sampled_at = Some(now);
        state.system = system;
        state.process = process;

        HostMetrics {
            cpu_percent,
            available_memory_percent,
            // Windows performance counters are intentionally not guessed here.
            disk_active_percent: None,
            disk_queue: None,
            process_working_set_bytes: working_set,
            process_private_bytes: private_bytes,
            process_read_bytes_per_second,
            process_write_bytes_per_second,
            disk_free_bytes,
        }
    }
}

#[cfg(target_os = "windows")]
fn filetime_ticks(value: windows_sys::Win32::Foundation::FILETIME) -> u64 {
    u64::from(value.dwLowDateTime) | (u64::from(value.dwHighDateTime) << 32)
}

/// A reasoned controller output, suitable for both logging and worker control.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControllerDecision {
    pub state: ControllerState,
    pub limits: InventoryLimits,
    pub paused: bool,
    pub reason: Option<PauseReason>,
    pub recovery_at: Option<Instant>,
    pub transitioned: bool,
}

pub struct AdaptiveIndexController {
    config: ControllerConfig,
    state: ControllerState,
    limits: InventoryLimits,
    hold_until: Option<Instant>,
    recovery_at: Option<Instant>,
    recovery_exponent: u32,
    last_reason: Option<PauseReason>,
}

impl AdaptiveIndexController {
    pub fn new(config: ControllerConfig, now: Instant) -> Self {
        let config = config.normalized();
        Self {
            limits: config.canary,
            config,
            state: ControllerState::Ramping,
            hold_until: Some(now + config.healthy_window),
            recovery_at: None,
            recovery_exponent: 0,
            last_reason: None,
        }
    }

    pub fn state(&self) -> ControllerState {
        self.state
    }

    pub fn limits(&self) -> InventoryLimits {
        self.limits
    }

    pub fn recovery_at(&self) -> Option<Instant> {
        self.recovery_at
    }

    pub fn observe(
        &mut self,
        now: Instant,
        observation: ControllerObservation,
    ) -> ControllerDecision {
        let previous = (self.state, self.limits, self.recovery_at);

        if observation.foreground_active {
            self.state = ControllerState::Paused(PauseReason::Foreground);
            self.recovery_at = None;
            self.last_reason = Some(PauseReason::Foreground);
            return self.decision(previous);
        }

        if matches!(self.state, ControllerState::Paused(PauseReason::Foreground)) {
            self.state = ControllerState::Ramping;
            self.hold_until = Some(now + self.config.healthy_window);
            self.last_reason = None;
        }

        if let Some(reason) = self.hard_reason(observation) {
            self.pause_for(now, reason);
            return self.decision(previous);
        }

        if matches!(self.state, ControllerState::Paused(_)) {
            if self
                .recovery_at
                .is_some_and(|recovery_at| now < recovery_at)
            {
                return self.decision(previous);
            }
            self.state = ControllerState::Ramping;
            self.limits = self.config.canary;
            self.recovery_at = None;
            self.hold_until = Some(now + self.config.healthy_window);
            self.last_reason = None;
        }

        if let Some(reason) = self.soft_reason(observation) {
            let hold_expired = self.hold_until.is_none_or(|hold_until| now >= hold_until);
            if !matches!(self.state, ControllerState::Throttled) || hold_expired {
                self.throttle(now, reason);
            }
            return self.decision(previous);
        }

        if self.hold_until.is_some_and(|hold_until| now >= hold_until) {
            self.increase_limits(now);
        }

        self.decision(previous)
    }

    fn hard_reason(&self, observation: ControllerObservation) -> Option<PauseReason> {
        if observation.foreground_error
            || observation.foreground_bad_quality
            || observation.inventory_error
        {
            return Some(PauseReason::OpcHealth);
        }
        if let Some(latency) = observation.foreground_latency_ms {
            let baseline = observation
                .baseline_latency_ms
                .unwrap_or(self.config.foreground_latency_absolute_ms);
            let hard_limit = self
                .config
                .foreground_latency_absolute_ms
                .max(baseline.saturating_mul(4));
            if latency >= hard_limit {
                return Some(PauseReason::OpcHealth);
            }
        }
        if observation
            .host_cpu_percent
            .is_some_and(|value| value >= 85.0)
        {
            return Some(PauseReason::HostCpu);
        }
        if observation
            .available_memory_percent
            .is_some_and(|value| value <= 8.0)
        {
            return Some(PauseReason::Memory);
        }
        if observation
            .disk_active_percent
            .is_some_and(|value| value >= 90.0)
            || observation.disk_queue.is_some_and(|value| value >= 5.0)
        {
            return Some(PauseReason::Disk);
        }
        if observation
            .database_commit_p95_ms
            .is_some_and(|value| value >= 1_000)
            || observation.insufficient_disk_space
        {
            return Some(PauseReason::Database);
        }
        None
    }

    fn soft_reason(&self, observation: ControllerObservation) -> Option<PauseReason> {
        if observation
            .foreground_latency_ms
            .zip(observation.baseline_latency_ms)
            .is_some_and(|(latency, baseline)| latency >= baseline.saturating_mul(2))
        {
            return Some(PauseReason::OpcHealth);
        }
        if observation
            .host_cpu_percent
            .is_some_and(|value| value >= 70.0)
        {
            return Some(PauseReason::HostCpu);
        }
        if observation
            .available_memory_percent
            .is_some_and(|value| value <= 15.0)
        {
            return Some(PauseReason::Memory);
        }
        if observation
            .disk_active_percent
            .is_some_and(|value| value >= 70.0)
            || observation.disk_queue.is_some_and(|value| value >= 2.0)
        {
            return Some(PauseReason::Disk);
        }
        if observation
            .database_commit_p95_ms
            .is_some_and(|value| value >= 250)
        {
            return Some(PauseReason::Database);
        }
        None
    }

    fn throttle(&mut self, now: Instant, reason: PauseReason) {
        self.state = ControllerState::Throttled;
        self.limits = InventoryLimits {
            item_rate_per_second: self
                .limits
                .item_rate_per_second
                .saturating_div(2)
                .max(self.config.floor.item_rate_per_second),
            batch_size: self
                .limits
                .batch_size
                .saturating_div(2)
                .max(self.config.floor.batch_size),
            duty_cycle_percent: self
                .limits
                .duty_cycle_percent
                .saturating_div(2)
                .max(self.config.floor.duty_cycle_percent),
        };
        self.hold_until = Some(now + self.config.healthy_window);
        self.recovery_at = None;
        self.last_reason = Some(reason);
    }

    fn pause_for(&mut self, now: Instant, reason: PauseReason) {
        self.state = ControllerState::Paused(reason);
        self.limits = self.config.canary;
        let multiplier = 1_u32
            .checked_shl(self.recovery_exponent.min(10))
            .unwrap_or(u32::MAX);
        let delay = self
            .config
            .recovery_delay
            .checked_mul(multiplier)
            .unwrap_or(self.config.maximum_recovery_delay)
            .min(self.config.maximum_recovery_delay);
        self.recovery_at = Some(now + delay);
        self.recovery_exponent = self.recovery_exponent.saturating_add(1);
        self.hold_until = None;
        self.last_reason = Some(reason);
    }

    fn increase_limits(&mut self, now: Instant) {
        if !self.config.unlimited_item_rate {
            self.limits.item_rate_per_second = self
                .limits
                .item_rate_per_second
                .saturating_add((self.limits.item_rate_per_second / 10).max(1))
                .min(self.config.ceiling.item_rate_per_second);
        }
        self.limits.batch_size = self
            .limits
            .batch_size
            .saturating_add((self.limits.batch_size / 10).max(1))
            .min(self.config.ceiling.batch_size);
        self.limits.duty_cycle_percent = self
            .limits
            .duty_cycle_percent
            .saturating_add(1)
            .min(self.config.ceiling.duty_cycle_percent);
        self.state = if self.limits == self.config.ceiling {
            ControllerState::Steady
        } else {
            ControllerState::Ramping
        };
        self.hold_until = Some(now + self.config.healthy_window);
        self.recovery_exponent = 0;
        self.last_reason = None;
    }

    fn decision(
        &self,
        previous: (ControllerState, InventoryLimits, Option<Instant>),
    ) -> ControllerDecision {
        ControllerDecision {
            state: self.state,
            limits: self.limits,
            paused: matches!(self.state, ControllerState::Paused(_)),
            reason: self.last_reason,
            recovery_at: self.recovery_at,
            transitioned: previous != (self.state, self.limits, self.recovery_at),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> Instant {
        Instant::now()
    }

    fn healthy() -> ControllerObservation {
        ControllerObservation {
            baseline_latency_ms: Some(100),
            foreground_latency_ms: Some(100),
            ..ControllerObservation::default()
        }
    }

    #[test]
    fn normalizes_invalid_limits_without_panicking() {
        let config = ControllerConfig {
            floor: InventoryLimits {
                item_rate_per_second: 0,
                batch_size: 0,
                duty_cycle_percent: 0,
            },
            canary: InventoryLimits {
                item_rate_per_second: 1,
                batch_size: 1,
                duty_cycle_percent: 1,
            },
            ceiling: InventoryLimits {
                item_rate_per_second: 0,
                batch_size: 0,
                duty_cycle_percent: 0,
            },
            unlimited_item_rate: false,
            healthy_window: Duration::ZERO,
            recovery_delay: Duration::ZERO,
            maximum_recovery_delay: Duration::ZERO,
            foreground_latency_absolute_ms: 0,
        }
        .normalized();
        assert_eq!(config.floor.item_rate_per_second, 1);
        assert_eq!(config.floor.batch_size, 1);
        assert_eq!(config.floor.duty_cycle_percent, 1);
        assert_eq!(config.canary, config.floor);
        assert_eq!(config.ceiling, config.floor);
        assert_eq!(config.healthy_window, Duration::from_millis(1));
    }

    #[test]
    fn preserves_unlimited_item_rate_when_adaptive_control_is_enabled() {
        let config = ControllerConfig {
            unlimited_item_rate: true,
            ..ControllerConfig::default()
        }
        .normalized();
        let started = now();
        let mut controller = AdaptiveIndexController::new(config, started);
        assert_eq!(controller.limits().item_rate_per_second, 0);
        let decision = controller.observe(started + Duration::from_secs(31), healthy());
        assert_eq!(decision.limits.item_rate_per_second, 0);
        assert_eq!(config.ceiling.item_rate_per_second, 0);
    }

    #[test]
    fn clamps_native_batch_limits_to_the_upstream_ceiling() {
        let oversized = MAX_NATIVE_INVENTORY_BATCH_SIZE + 1;
        let config = ControllerConfig {
            floor: InventoryLimits {
                item_rate_per_second: 1,
                batch_size: oversized,
                duty_cycle_percent: 1,
            },
            canary: InventoryLimits {
                item_rate_per_second: 1,
                batch_size: oversized,
                duty_cycle_percent: 1,
            },
            ceiling: InventoryLimits {
                item_rate_per_second: 1,
                batch_size: u32::MAX,
                duty_cycle_percent: 1,
            },
            ..ControllerConfig::default()
        }
        .normalized();

        assert_eq!(config.floor.batch_size, MAX_NATIVE_INVENTORY_BATCH_SIZE);
        assert_eq!(config.canary.batch_size, MAX_NATIVE_INVENTORY_BATCH_SIZE);
        assert_eq!(config.ceiling.batch_size, MAX_NATIVE_INVENTORY_BATCH_SIZE);
    }

    #[test]
    fn starts_at_the_canary_profile_and_increases_after_healthy_window() {
        let started = now();
        let config = ControllerConfig {
            healthy_window: Duration::from_secs(10),
            ..ControllerConfig::default()
        };
        let mut controller = AdaptiveIndexController::new(config, started);
        assert_eq!(controller.state(), ControllerState::Ramping);
        assert_eq!(controller.limits(), config.canary);

        let before = controller.observe(started + Duration::from_secs(9), healthy());
        assert!(!before.transitioned);
        let after = controller.observe(started + Duration::from_secs(10), healthy());
        assert!(after.transitioned);
        assert!(after.limits.item_rate_per_second > config.canary.item_rate_per_second);
    }

    #[test]
    fn soft_pressure_halves_limits_and_holds_them() {
        let started = now();
        let mut controller = AdaptiveIndexController::new(ControllerConfig::default(), started);
        let decision = controller.observe(
            started + Duration::from_secs(1),
            ControllerObservation {
                host_cpu_percent: Some(75.0),
                ..healthy()
            },
        );
        assert_eq!(decision.state, ControllerState::Throttled);
        assert_eq!(decision.reason, Some(PauseReason::HostCpu));
        assert!(decision.limits.item_rate_per_second < 50);
        let held = controller.observe(
            started + Duration::from_secs(2),
            ControllerObservation {
                host_cpu_percent: Some(50.0),
                ..healthy()
            },
        );
        assert_eq!(held.state, ControllerState::Throttled);
        assert_eq!(held.limits, decision.limits);
    }

    #[test]
    fn hard_pressure_pauses_and_recovers_at_canary() {
        let started = now();
        let config = ControllerConfig {
            recovery_delay: Duration::from_secs(5),
            ..ControllerConfig::default()
        };
        let mut controller = AdaptiveIndexController::new(config, started);
        let paused = controller.observe(
            started + Duration::from_secs(1),
            ControllerObservation {
                foreground_bad_quality: true,
                ..healthy()
            },
        );
        assert_eq!(
            paused.state,
            ControllerState::Paused(PauseReason::OpcHealth)
        );
        assert!(paused.paused);
        let held = controller.observe(started + Duration::from_secs(6), healthy());
        assert_eq!(held.state, ControllerState::Ramping);
        assert_eq!(held.limits, config.canary);
        assert!(!held.paused);
    }

    #[test]
    fn foreground_work_pauses_without_consuming_health_backoff() {
        let started = now();
        let mut controller = AdaptiveIndexController::new(ControllerConfig::default(), started);
        let paused = controller.observe(
            started + Duration::from_secs(1),
            ControllerObservation {
                foreground_active: true,
                ..healthy()
            },
        );
        assert_eq!(
            paused.state,
            ControllerState::Paused(PauseReason::Foreground)
        );
        assert_eq!(paused.recovery_at, None);
        let resumed = controller.observe(started + Duration::from_secs(2), healthy());
        assert_eq!(resumed.state, ControllerState::Ramping);
        assert_eq!(resumed.recovery_at, None);
    }

    #[test]
    fn hard_guardrails_have_typed_priority_and_unavailable_metrics_are_ignored() {
        let started = now();
        let mut controller = AdaptiveIndexController::new(ControllerConfig::default(), started);
        let decision = controller.observe(
            started + Duration::from_secs(1),
            ControllerObservation {
                host_cpu_percent: Some(95.0),
                available_memory_percent: Some(50.0),
                disk_active_percent: None,
                ..healthy()
            },
        );
        assert_eq!(decision.reason, Some(PauseReason::HostCpu));

        let mut controller = AdaptiveIndexController::new(ControllerConfig::default(), started);
        let decision = controller.observe(
            started + Duration::from_secs(1),
            ControllerObservation {
                available_memory_percent: Some(50.0),
                ..healthy()
            },
        );
        assert!(!decision.paused);
    }

    #[test]
    fn unavailable_host_metrics_are_explicit() {
        assert_eq!(UnavailableHostMetrics.snapshot(), HostMetrics::default());
    }
}
