from twitter_archive_processor.archive import Archive

class Processor:
    def __init__(self, archive_dir: Path):
        self.archive_dir = archive_dir
        self.archives = []

    def process_archives(self):
        for archive_file in self.archive_dir.glob("*.json"):
            archive = Archive(archive_file)
            archive.load()
            self.archives.append(archive)

    def generate_reports(self):
        # Logic to generate reports from processed archives
        pass