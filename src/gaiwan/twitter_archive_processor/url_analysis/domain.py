from dataclasses import dataclass, field
from typing import Dict, Union, List, Callable, Set

class DomainNormalizer:
    """Normalizes domain names for consistent analysis."""
    
    def __init__(self):
        # Common domain mappings (e.g., youtu.be -> youtube.com)
        self.domain_mappings = {
            'youtu.be': 'youtube.com',
            't.co': 'twitter.com',
            'goo.gl': 'google.com',
            'bit.ly': 'bitly.com',
            'amzn.to': 'amazon.com',
            'tinyurl.com': 'tinyurl.com',
            'ift.tt': 'ifttt.com',
        }
    
    def normalize(self, domain: str) -> str:
        """Normalize a domain name."""
        try:
            # Remove www. prefix if present
            domain = domain.lower().strip()
            if domain.startswith('www.'):
                domain = domain[4:]
            
            # Check domain mappings first
            if domain in self.domain_mappings:
                return self.domain_mappings[domain]
            
            # Split domain into parts
            parts = domain.split('.')
            
            # Handle special cases
            if len(parts) >= 2:
                # Common TLDs that should be preserved
                if parts[-2] + '.' + parts[-1] in {
                    'co.uk', 'co.jp', 'com.au', 'co.nz', 
                    'org.uk', 'gov.uk', 'ac.uk'
                }:
                    if len(parts) > 2:
                        return '.'.join(parts[-3:])
                    return domain
                
                # For other domains, return last two parts
                return '.'.join(parts[-2:])
            
            return domain
            
        except Exception as e:
            logger.warning(f"Error normalizing domain '{domain}': {e}")
            return domain  # Return original domain if normalization fails

    def is_shortener(self, domain: str) -> bool:
        """Check if domain is a known URL shortener."""
        return domain in self.shortener_domains