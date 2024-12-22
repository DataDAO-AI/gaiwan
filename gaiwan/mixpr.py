class MixPRConfig:
    """Configuration for mixing pull requests."""
    
    def __init__(self):
        self.base_url = "https://api.github.com"
        self.token = None
        self.repository = None
        self.branch = None

    @classmethod
    def from_env(cls):
        """Create config from environment variables."""
        config = cls()
        return config 