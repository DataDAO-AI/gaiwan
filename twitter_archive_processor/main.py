#Handles Arguments and calls functions from other modules
import argparse
import extraction as ex
import transformation as tr
import export as expt
import os

def main(archive_path: str, output_dir: str, output_formats: list[str], system_message: str):
    # 1. Extract all data from the archive
    data = ex.extract_archive_data(archive_path)

    # 2. Combine tweets/likes into a single dict
    all_content = tr.combine_all_content(data)

    # 3. Identify ConvoThreads and conversations
    ConvoThreads, conversations = ex.extract_ConvoThreads_and_conversations(all_content)

    # 4. Export as requested
    if 'markdown' in output_formats:
        # Create necessary directories
        ConvoThreads_output_dir = os.path.join(output_dir, 'ConvoThreads')
        images_folder = os.path.join(output_dir, 'images')
        tweets_by_date_dir = os.path.join(output_dir, 'tweets_by_date')
        
        os.makedirs(ConvoThreads_output_dir, exist_ok=True)
        os.makedirs(images_folder, exist_ok=True)
        os.makedirs(tweets_by_date_dir, exist_ok=True)

        # Save ConvoThreads
        for ConvoThread in ConvoThreads:
            expt.save_ConvoThread_markdown(ConvoThread, ConvoThreads_output_dir, os.path.join(archive_path, 'data', 'tweets_media'), images_folder)
        
        # Save standalone tweets by date
        expt.save_tweets_by_date(all_content, ConvoThreads, tweets_by_date_dir, images_folder)

    if 'oai' in output_formats:
        # Save as OpenAI JSONL
        oai_output_path = os.path.join(output_dir, 'conversations_oai.jsonl')
        expt.save_conversations_to_jsonl(ConvoThreads, conversations, oai_output_path, system_message)


    if __name__ == "__main__":
        parser = argparse.ArgumentParser(description="Process Twitter archive")
        parser.add_argument("--archive-path", default="test", help="Path to the Twitter archive directory")
        parser.add_argument("--output-dir", default="output", help="Directory where outputs will be saved")
        parser.add_argument("--output-formats", nargs='+', default=['markdown', 'oai'],
                            help="Output formats to generate (markdown, oai)")
        parser.add_argument("--system-message", default="You have been uploaded to the internet", 
                            help="System message for the conversation")
        args = parser.parse_args()

        main(args.archive_path, args.output_dir, args.output_formats, args.system_message)