from dataclasses import dataclass, field
from typing import Dict, Union, List, Callable, Set

class DomainNormalizer:
    """Normalizes domain names for consistent analysis."""
    
    def __init__(self):
        self.domain_mappings = {
            'twitter.com': ['twitter.com', 'www.twitter.com', 'm.twitter.com'],
            'youtube.com': ['youtube.com', 'www.youtube.com', 'youtu.be', 'm.youtube.com'],
            'wikipedia.org': ['wikipedia.org', 'en.wikipedia.org', 'fr.wikipedia.org', 'de.wikipedia.org'],
            'github.com': ['github.com', 'raw.githubusercontent.com', 'gist.github.com'],
            'medium.com': lambda d: d.endswith('.medium.com'),
            'substack.com': lambda d: d.endswith('.substack.com')
        }
        
        # Known URL shortener domains
        self.shortener_domains = {
            't.co', 'bit.ly', 'goo.gl', 'tinyurl.com',
            'ow.ly', 'buff.ly', 'dlvr.it', 'is.gd',
            'tiny.cc', 'j.mp', 'ift.tt', 'amzn.to'
        }
    
    def normalize(self, domain: str) -> str:
        """Normalize domain names."""
        domain = domain.lower()
        if domain.startswith('www.'):
            domain = domain[4:]
            
        # Special case for Twitter/X domains
        if domain == 'x.com' or domain.endswith('.x.com') or domain == 'twitter.com' or domain.endswith('.twitter.com'):
            return 'twitter.com'
            
        # Check each mapping
        for normalized, variants in self.domain_mappings.items():
            if isinstance(variants, list):
                if domain in variants:
                    return normalized
            elif callable(variants):
                if variants(domain):
                    return normalized
            
            # Special case for Wikipedia language subdomains
            if normalized == 'wikipedia.org' and domain.endswith('.wikipedia.org'):
                return normalized
                
        return domain

    def is_shortener(self, domain: str) -> bool:
        """Check if domain is a known URL shortener."""
        return domain in self.shortener_domains