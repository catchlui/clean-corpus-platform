"""PII detection package.

Auto-registers built-in detectors on import so they're available for all datasets.
"""

# Auto-register built-in detectors
def _auto_register_detectors():
    """Register built-in PII detectors automatically."""
    try:
        from .registry import register_detector
        from .detectors.email import EmailDetector
        from .detectors.phone import PhoneDetector
        from .detectors.aadhaar import AadhaarDetector
        
        # Register if not already registered
        from .registry import list_detectors
        registered = list_detectors()
        
        if 'email' not in registered:
            register_detector(EmailDetector())
        if 'phone' not in registered:
            register_detector(PhoneDetector())
        if 'aadhaar' not in registered:
            register_detector(AadhaarDetector())
    except ImportError:
        # Detectors may not be available, that's okay
        pass

# Auto-register on import
_auto_register_detectors()
