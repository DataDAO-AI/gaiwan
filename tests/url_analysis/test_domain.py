import pytest
from gaiwan.twitter_archive_processor.url_analysis.domain import DomainNormalizer

def test_domain_normalization():
    normalizer = DomainNormalizer()
    
    # Test basic normalization
    assert normalizer.normalize('www.youtube.com') == 'youtube.com'
    assert normalizer.normalize('youtu.be') == 'youtube.com'
    assert normalizer.normalize('m.youtube.com') == 'youtube.com'
    
    # Test Twitter domains
    assert normalizer.normalize('twitter.com') == 'twitter.com'
    assert normalizer.normalize('x.com') == 'twitter.com'
    assert normalizer.normalize('subdomain.x.com') == 'twitter.com'  # Test x.com subdomain
    assert normalizer.normalize('api.twitter.com') == 'twitter.com'  # Test twitter.com subdomain
    
    # Test Wikipedia
    assert normalizer.normalize('en.wikipedia.org') == 'wikipedia.org'
    assert normalizer.normalize('fr.wikipedia.org') == 'wikipedia.org'

def test_shortener_detection():
    normalizer = DomainNormalizer()
    
    assert normalizer.is_shortener('t.co') == True
    assert normalizer.is_shortener('bit.ly') == True
    assert normalizer.is_shortener('youtube.com') == False