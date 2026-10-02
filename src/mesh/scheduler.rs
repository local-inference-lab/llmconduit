#![allow(dead_code)]

use crate::mesh::registry::MeshRegistry;
use crate::mesh::registry::MeshReservation;

#[derive(Debug, Clone)]
pub struct MeshScheduler {
    registry: MeshRegistry,
}

impl MeshScheduler {
    pub fn new(registry: MeshRegistry) -> Self {
        Self { registry }
    }

    pub fn reserve(&self, model: &str) -> Result<MeshReservation, ScheduleError> {
        self.registry
            .reserve(model)
            .ok_or(ScheduleError::CapacityExhausted)
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ScheduleError {
    #[error("mesh capacity exhausted")]
    CapacityExhausted,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AvailabilitySchedule;
    use crate::mesh::protocol::ModelAdvertisement;
    use crate::mesh::protocol::ResourceAdvertisement;
    use crate::mesh::protocol::WorkerAdvertisement;
    use iroh::SecretKey;
    use std::time::Duration;

    fn advertisement(model: &str, capacity: u32) -> WorkerAdvertisement {
        WorkerAdvertisement {
            protocol_version: crate::mesh::protocol::PROTOCOL_VERSION,
            node_name: None,
            agent_version: "test".to_string(),
            resources: vec![ResourceAdvertisement {
                resource_id: "primary".to_string(),
                models: vec![ModelAdvertisement {
                    id: model.to_string(),
                    context_limit: None,
                }],
                availability: AvailabilitySchedule::default(),
                effective_capacity: capacity,
                accepting_requests: true,
                healthy: true,
                revision: 1,
            }],
            model_switching: None,
            request_encodings: Vec::new(),
        }
    }

    #[test]
    fn scheduler_reports_capacity_exhaustion_stably() {
        let registry = MeshRegistry::new(Duration::from_secs(30));
        let scheduler = MeshScheduler::new(registry.clone());
        assert_eq!(
            scheduler.reserve("missing").unwrap_err(),
            ScheduleError::CapacityExhausted
        );
    }

    #[test]
    fn scheduler_uses_registry_reservations() {
        let registry = MeshRegistry::new(Duration::from_secs(30));
        let scheduler = MeshScheduler::new(registry.clone());
        let endpoint = SecretKey::generate().public();
        registry.register_test(endpoint, advertisement("model-a", 1));

        let reservation = scheduler.reserve("model-a").expect("reservation");
        assert_eq!(reservation.resource.resource_id, "primary");
        assert_eq!(
            scheduler.reserve("model-a").unwrap_err(),
            ScheduleError::CapacityExhausted
        );
        drop(reservation);
        assert!(scheduler.reserve("model-a").is_ok());
    }
}
