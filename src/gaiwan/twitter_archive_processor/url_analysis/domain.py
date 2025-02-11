from dataclasses import dataclass, field
from typing import Dict, Union, List, Callable, Set

class DomainNormalizer:
    """Normalizes domain names for consistent analysis."""
    
    def __init__(self):
        self.domain_mappings = {
            'twitter.com': ['twitter.com', 'x.com', 'www.twitter.com', 'm.twitter.com'],
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
        """Normalize a domain to its canonical form."""
        domain = domain.lower()
        
        # Remove www. prefix
        if domain.startswith('www.'):
            domain = domain[4:]
            
        # Check direct mappings
        for canonical, variants in self.domain_mappings.items():
            if isinstance(variants, list) and domain in variants:
                return canonical
            elif callable(variants) and variants(domain):
                return canonical
                
        return domain

    def is_shortener(self, domain: str) -> bool:
        """Check if domain is a known URL shortener."""
        return domain in self.shortener_domains