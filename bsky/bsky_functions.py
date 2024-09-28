import pandas as pd

def parse_feedviewpost_list(feedviewpost_list):
    """
    Parses a list of FeedViewPost objects into a pandas DataFrame.

    Parameters:
        feedviewpost_list (list): A list of FeedViewPost objects.

    Returns:
        pd.DataFrame: A DataFrame containing parsed data from the posts.
    """
    data = []

    for post_obj in feedviewpost_list:
        try:
            post_data = {}
            post = post_obj.post
            record = post.record
            author = post.author
            viewer = post.viewer

            # Basic post information
            post_data['post_uri'] = post.uri
            post_data['post_cid'] = post.cid
            post_data['post_created_at'] = record.created_at
            post_data['post_text'] = record.text
            post_data['post_indexed_at'] = post.indexed_at

            # Author information
            post_data['author_did'] = author.did
            post_data['author_handle'] = author.handle
            post_data['author_display_name'] = author.display_name
            post_data['author_created_at'] = author.created_at
            post_data['author_avatar'] = author.avatar

            # Engagement metrics
            post_data['like_count'] = post.like_count
            post_data['repost_count'] = post.repost_count
            post_data['reply_count'] = post.reply_count
            post_data['quote_count'] = post.quote_count

            # Viewer interaction
            post_data['viewer_liked'] = viewer.like is not None
            post_data['viewer_reposted'] = viewer.repost is not None
            post_data['viewer_thread_muted'] = viewer.thread_muted

            # Embed information (if any)
            embed = post.embed
            if embed:
                if hasattr(embed, 'images') and embed.images:
                    post_data['embed_type'] = 'images'
                    post_data['embed_count'] = len(embed.images)
                elif hasattr(embed, 'record') and embed.record:
                    post_data['embed_type'] = 'record'
                    post_data['embed_record_uri'] = embed.record.uri
                else:
                    post_data['embed_type'] = 'other'
            else:
                post_data['embed_type'] = None

            # Add the post data to the list
            data.append(post_data)

        except Exception as e:
            print(f"Error parsing post: {e}")
            continue

    # Create DataFrame
    df = pd.DataFrame(data)
    return df