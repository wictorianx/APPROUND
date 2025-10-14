from kickapi import KickAPI

def get_vods_for_channel(username: str):
    api = KickAPI()
    channel = api.channel(username)
    # `channel.videos` likely gives list of Video objects (past streams)
    vods = channel.videos  
    results = []
    for vid in vods:
        # Each video object has attributes like `vid.stream` (URL) etc.
        results.append({
            "id": vid.id,
            "title": vid.title,
            "stream_url": vid.stream,
        })
    return results
username = "jahrein"
vods = get_vods_for_channel(username)
for v in vods:
    print(v["title"], v["stream_url"])
