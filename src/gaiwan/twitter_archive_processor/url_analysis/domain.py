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
            'x.com': 'twitter.com',  # Add Twitter's rebranding
        }
        
        # Known URL shortener domains
        self.shortener_domains = {
            't.co', 'bit.ly', 'goo.gl', 'tinyurl.com',
            'ow.ly', 'buff.ly', 'dlvr.it', 'is.gd',
            'tiny.cc', 'j.mp', 'ift.tt', 'amzn.to'
        }
    
    def normalize(self, domain: str) -> str:
        """Normalize a domain name."""
        try:
            # Remove www. prefix if present
            domain = domain.lower().strip()
            if domain.startswith('www.'):
                domain = domain[4:]
            
            # Split domain into parts
            parts = domain.split('.')
            
            # Handle special cases
            if len(parts) >= 2:
                # Check if the base domain (last two parts) is in mappings
                base_domain = '.'.join(parts[-2:])
                if base_domain in self.domain_mappings:
                    return self.domain_mappings[base_domain]
                
                # Common TLDs that should be preserved
                if base_domain in {
                    'co.uk', 'co.jp', 'com.au', 'co.nz', 
                    'org.uk', 'gov.uk', 'ac.uk'
                }:
                    if len(parts) > 2:
                        return '.'.join(parts[-3:])
                    return domain
                
                # For other domains, return last two parts
                return base_domain
            
            return domain
            
        except Exception as e:
            logger.warning(f"Error normalizing domain '{domain}': {e}")
            return domain  # Return original domain if normalization fails

    def is_shortener(self, domain: str) -> bool:
        """Check if domain is a known URL shortener."""
        return domain in self.shortener_domains