# gaiwan
gaiwan consists of three main components that work together to process Twitter archives:

1. **Archive Processor** - Downloads and normalizes Twitter archive data
2. **Conversation Analyzer** - Builds and analyzes conversation threads
3. **Stats Collector** - Generates statistics about the archives

Here's a step-by-step guide to exercise the full functionality:

1. First, set up a working directory and create necessary folders:

```bash
mkdir twitter_analysis
cd twitter_analysis
mkdir archives output stats
```

2. Download and process archives:

```bash
python archive_processor.py archives/ --all
```

This will:
- Download archives from Supabase for all available accounts
- Process them into normalized format
- Create two main output files:
  - `archives/canonical_tweets.jsonl`
  - `archives/reply_edges.jsonl`
- Generate stats in `archives/stats/`

Alternative ways to run the archive processor:

```bash
# Process specific usernames
python archive_processor.py archives/ --usernames user1 user2 user3

# Process usernames from a file
python archive_processor.py archives/ --username-file usernames.txt

# Force reprocessing of already processed archives
python archive_processor.py archives/ --all --force-reprocess
```

3. Analyze conversations:

```bash
# Basic search and display
python conversation_analyzer.py \
    archives/canonical_tweets.jsonl \
    archives/reply_edges.jsonl \
    --search "from:specificuser #python"

# Search and save results
python conversation_analyzer.py \
    archives/canonical_tweets.jsonl \
    archives/reply_edges.jsonl \
    --search "machine learning filter:links" \
    --output output/ml_conversations.jsonl
```

Example search queries:
```bash
# Tweets containing specific words
--search "python programming"

# Exact phrases
--search '"machine learning" python'

# From specific accounts
--search "from:username python"

# With hashtags
--search "#python #coding"

# Excluding words
--search "python -javascript"

# Complex queries
--search 'from:user1 to:user2 "interesting topic" #tech -spam'
```

4. View statistics:
The stats are automatically generated during archive processing and saved in the `archives/stats/` directory. Each archive gets its own stats file named `{username}_archive_stats.json`.

Sample workflow putting it all together:

```bash
# 1. Create directories
mkdir -p twitter_analysis/{archives,output,stats}
cd twitter_analysis

# 2. Download and process archives
python archive_processor.py archives/ --all

# 3. Search for interesting conversations
python conversation_analyzer.py \
    archives/canonical_tweets.jsonl \
    archives/reply_edges.jsonl \
    --search 'from:techuser "machine learning" #AI filter:links' \
    --output output/ml_discussions.jsonl

# 4. Check the generated files
ls -l archives/stats/
cat output/ml_discussions.jsonl | jq  # if you have jq installed
```