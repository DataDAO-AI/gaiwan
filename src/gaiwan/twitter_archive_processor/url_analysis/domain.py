from dataclasses import dataclass, field
from typing import Dict, Union, List, Callable, Set

@dataclass
class DomainNormalizer:
    """Handles domain normalization rules and URL shortener detection."""
    
    shortener_domains: Set[str] = field(default_factory=lambda: {
        't.co', 'bit.ly', 'buff.ly', 'tinyurl.com',
        'ow.ly', 'goo.gl', 'tiny.cc', 'is.gd'
    })
    
    domain_groups: Dict[str, Union[List[Union[str, Callable]], Callable]] = field(default_factory=lambda: {
        'twitter.com': [
            'twitter.com', 'x.com',  # Direct matches
            'www.twitter.com', 'm.twitter.com',
            lambda d: d.endswith('.x.com'),  # Subdomains of x.com
            lambda d: d.endswith('.twitter.com')  # Subdomains of twitter.com
        ],
        'youtube.com': [
            'youtube.com', 'www.youtube.com', 
            'youtu.be', 'm.youtube.com'
        ],
        'wikipedia.org': lambda d: d.endswith("wikipedia.org"),
        'github.com': [
            'github.com', 'raw.githubusercontent.com',
            'gist.github.com', 'm.github.com'
        ]
    })

    def normalize(self, domain: str) -> str:
        """Normalize domain names to group related sites."""
        domain = domain.lower().replace('www.', '')
        
        # Handle mobile domains
        parts = domain.split('.')
        if 'm' in parts and parts.index('m') < len(parts) - 2:
            parts.pop(parts.index('m'))
            domain = '.'.join(parts)
        
        # Check domain groups
        for main_domain, matchers in self.domain_groups.items():
            if isinstance(matchers, list):
                for matcher in matchers:
                    if callable(matcher):
                        if matcher(domain):
                            return main_domain
                    elif domain == matcher:  # Direct string comparison
                        return main_domain
            elif callable(matchers):
                if matchers(domain):
                    return main_domain
                    
        return domain

    def is_shortener(self, domain: str) -> bool:
        """Check if domain is a known URL shortener."""
        return domain in self.shortener_domains