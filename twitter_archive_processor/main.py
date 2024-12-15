""" Handles Arguments and calls functions from other modules """
import argparse
import os

import twitter_archive_processor.extraction as ex
import twitter_archive_processor.transformation as tr
import twitter_archive_processor.export as expt

def main(archive_path: str, output_dir: str, output_formats: list[str], system_message: str):
    """ main function to process data using code from other modules """
    # 1. Extract all data from the archive
    data = ex.extract_archive_data(archive_path)

    # 2. Combine tweets/likes into a single dict
    all_content = tr.combine_all_content(data)

    # 3. Identify ConvoThreads and conversations
    convo_threads, conversations = tr.extract_threads_and_conversations(all_content)

    # 4. Export as requested
    if 'markdown' in output_formats:
        # Create necessary directories
        convo_threads_output_dir = os.path.join(output_dir, 'ConvoThreads')
        images_folder = os.path.join(output_dir, 'images')
        tweets_by_date_dir = os.path.join(output_dir, 'tweets_by_date')

        os.makedirs(convo_threads_output_dir, exist_ok=True)
        os.makedirs(images_folder, exist_ok=True)
        os.makedirs(tweets_by_date_dir, exist_ok=True)

        # Save ConvoThreads
        for ct in convo_threads:
            expt.save_convo_threads_as_markdown(
                ct, convo_threads_output_dir,
                os.path.join(archive_path, 'data', 'tweets_media'),
                images_folder)

        # Save standalone tweets by date
        expt.save_tweets_by_date(all_content, convo_threads, tweets_by_date_dir, images_folder)

    if 'oai' in output_formats:
        # Save as OpenAI JSONL
        oai_output_path = os.path.join(output_dir, 'conversations_oai.jsonl')
        expt.save_conversations_to_jsonl(convo_threads, conversations,
                                         oai_output_path, system_message)


    if __name__ == "__main__":
        parser = argparse.ArgumentParser(description="Process Twitter archive")
        parser.add_argument("--archive-path", default="test",
                            help="Path to the Twitter archive directory")
        parser.add_argument("--output-dir", default="output",
                            help="Directory where outputs will be saved")
        parser.add_argument("--output-formats", nargs='+', default=['markdown', 'oai'],
                            help="Output formats to generate (markdown, oai)")
        parser.add_argument("--system-message", default="You have been uploaded to the internet",
                            help="System message for the conversation")
        args = parser.parse_args()

        main(args.archive_path, args.output_dir, args.output_formats, args.system_message)
