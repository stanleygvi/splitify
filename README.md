# Splitify

Splitify is a tool that turns one Spotify playlist into multiple smaller playlists based on musical similarity.

Instead of splitting tracks randomly, Splitify looks at audio traits like energy, mood, danceability, tempo, acousticness, and more, then groups songs that feel like they belong together.

## What you can do with Splitify

- Log in with your Spotify account
- Choose one or more playlists from your library
- Pick the kind of split you want, such as `Balanced`, `Energy`, `Mood`, or `Danceability`
- Fine-tune the audio traits manually if you want more control
- Generate new playlists automatically from the results

## How it works

1. You sign in with Spotify.
2. Splitify loads your playlists.
3. You choose a playlist and select how you want it split.
4. Splitify analyzes the tracks using Spotify-related audio data.
5. New playlists are created for each group of similar songs.

Splitify creates new playlists instead of changing your original playlist.

## What gets created

When a playlist is processed, Splitify creates new Spotify playlists for the groups it finds. These groups are based on traits such as:

- Energy
- Mood
- Danceability
- Tempo
- Acousticness
- Instrumentalness
- Speechiness
- Liveness
- Loudness

Some very small or low-quality groups may be skipped so the final playlists are more useful.

## Spotify permissions

Splitify currently asks for these Spotify permissions:

- `user-read-private`
- `playlist-read-private`
- `playlist-modify-public`
- `playlist-modify-private`

These permissions are used so Splitify can:

- identify your Spotify account
- read the playlists you choose
- create new playlists for the split results

## Privacy and data use

Splitify uses your Spotify access only to power the playlist-splitting workflow.

- Your existing playlists are read so tracks can be analyzed and grouped.
- Splitify creates new playlists for the output.
- Your original playlists are not edited by the splitting flow.

## Who this is for

Splitify is useful if you want to:

- break up a long playlist into smaller vibe-based sets
- separate high-energy songs from calmer ones
- pull apart mixed playlists into more consistent moods
- discover natural clusters inside a playlist you already love

## Summary

Splitify helps you reorganize Spotify playlists into cleaner, more focused listening experiences without manually sorting every track yourself.

## Disclaimer

Splitify unfortunately cannot be fully public-facing at the moment because of Spotify API access restrictions and app approval requirements. Access may need to remain limited to approved users while those platform restrictions are in place.
