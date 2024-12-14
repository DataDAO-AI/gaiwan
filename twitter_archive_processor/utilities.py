#Shared Utilities to clean text, process media files, and set up logging
import re

def clean_text(text: str, entities: Optional[Dict[str, Any]] = None) -> str:
    if entity and 'urls' in entities:
        for url_obj in entities['urls']:    
            short_url = url_obj.get('url', '')
            full_url = url_obj.get('expanded_url', '')
            if short_url and full_url:
                text = text.replace(short_url, full_url)
    
    text = re.sub(r'https://t.co/\w+', '', text) #t.co link removal
    text = re.sub(r'@\w+', '', text) #mentions removal
    text = re.sub(r'#\w+', '', text) #hashtags removal  May want to include this on occasion
    text = re.sub(r'\s+', ' ', text) #extra spaces removal  
    #may want to add more cleaning steps as needed

    return text.strip()

def process_single_file(file_path: str, extractor: callable, media_dir: str, content_source: str) -> List[Tweet]:
    #Load the file
    data = load_json_file(file_path)
    if data is None:
        return []
    #Extract the tweets
    return extractor(data, media_dir, content_source)

def process_data_for_type(archive_path: str, data_type: str, media_dir: str, content_source: str) -> List[Tweet]:
   media_folder = os.path.join(archive_path, 'data', 'tweets_media')
   contents =[]
   files_info = type_info.get('files', [])

   file_path = [os.path.join(archive_path, file_info['fileName']) for file_info in files_info]
    with ProcessPoolExecutor() as executor:
        for file_name in data_files:
            file_path = os.path.join(archive_path, 'data', file_name)
            future = executor.submit(process_single_file, file_path, extract_tweets, media_dir, content_source)
            futures.append(future)
    #Collect the results
    results = []
    for future in as_completed(futures):
        results.extend(future.result())
    return results