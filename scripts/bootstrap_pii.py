"""Bootstrap to register built-in PII detectors.

Call this (or import it) before running `clean-corpus build` so PIIPolicyGate can detect signals.
"""

from clean_corpus.pii.registry import register_detector
from clean_corpus.pii.detectors.email import EmailDetector
from clean_corpus.pii.detectors.phone import PhoneDetector
from clean_corpus.pii.detectors.aadhaar import AadhaarDetector

def register_default_detectors():
    register_detector(EmailDetector())
    register_detector(PhoneDetector())
    register_detector(AadhaarDetector())

if __name__ == "__main__":
    register_default_detectors()
    print("Registered detectors: email, phone, aadhaar")
