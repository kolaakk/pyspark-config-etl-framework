from __future__ import annotations

from etl_framework.dq.checks.load_completeness import LoadCompletenessCheck
from etl_framework.dq.checks.delta_integrity import DeltaProcessingIntegrityCheck
from etl_framework.dq.checks.accuracy_recon import DataAccuracyReconciliationCheck
from etl_framework.dq.checks.business_validity import BusinessRuleValidityCheck
from etl_framework.dq.checks.uniqueness import UniquenessDuplicateCheck
from etl_framework.dq.checks.referential_integrity import ReferentialIntegrityCheck
from etl_framework.dq.checks.historical_trend import HistoricalTrendAnomalyCheck
from etl_framework.dq.checks.timeliness_sla import TimelinessSLACheck
from etl_framework.dq.checks.privacy_gdpr import PrivacySecurityGDPRCheck


CHECKS = {
    "load_completeness": LoadCompletenessCheck,
    "delta_integrity": DeltaProcessingIntegrityCheck,
    "accuracy_recon": DataAccuracyReconciliationCheck,
    "business_validity": BusinessRuleValidityCheck,
    "uniqueness": UniquenessDuplicateCheck,
    "referential_integrity": ReferentialIntegrityCheck,
    "historical_trend": HistoricalTrendAnomalyCheck,
    "timeliness_sla": TimelinessSLACheck,
    "privacy_gdpr": PrivacySecurityGDPRCheck,
}
