"""
chat_vote_server.py
====================

Reads a YouTube Live Chat in real time, counts votes ("CR7" -> Ronaldo,
"LM10" -> Messi), tracks who's commenting, and broadcasts all of it to
the Ronaldo vs Messi spin-wheel game (ronaldo-vs-messi-live-spin.html)
over a local WebSocket connection.

The HTML game already tries to connect to ws://localhost:8765 on load,
so once this script is running the wheel's odds, the live commenter
feed, and the "top commenter" spotlight will all update live — no
changes needed on the frontend.

------------------------------------------------------------------
INSTALL
------------------------------------------------------------------
    pip install pytchat websockets

------------------------------------------------------------------
RUN
------------------------------------------------------------------
    python chat_vote_server.py --video-id YOUR_VIDEO_ID

The video ID is the part after "v=" in your live stream's URL, e.g.
for https://www.youtube.com/watch?v=dQw4w9WgXcQ the ID is dQw4w9WgXcQ.
The stream must already be live and public/unlisted (not private) for
pytchat to be able to read its chat.

Optional flags:
    --host        WebSocket host (default: localhost)
    --port        WebSocket port (default: 8765)
    --interval    How often to broadcast updates, in seconds (default: 1.0)
    --top-window  Seconds per "top commenter" window (default: 120 = 2 min)

Press Ctrl+C to stop the script cleanly.
------------------------------------------------------------------
"""

import argparse
import asyncio
import json
import threading
import time
from collections import deque
from dataclasses import dataclass, field

import pytchat
import websockets

# How many recent unique commenter names to keep for the on-screen feed.
RECENT_COMMENTERS_MAX = 14


@dataclass
class VoteCounter:
    """Thread-safe running tally of chat votes, commenters, and top-commenter window."""
    ronaldo: int = 0
    messi: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    # Every author's total comment count for the CURRENT top-commenter window
    # (reset every `top_window` seconds by reset_top_window_loop below).
    _window_counts: dict = field(default_factory=dict)

    # Most recent commenter names, newest first, for the live feed strip.
    _recent_commenters: deque = field(default_factory=lambda: deque(maxlen=RECENT_COMMENTERS_MAX))

    def register(self, author: str, message: str) -> None:
        """Inspect one chat message and update votes, the commenter feed, and the top-commenter window."""
        text = message.lower()

        with self._lock:
            # CR7 => a vote for Ronaldo
            if "cr7" in text:
                self.ronaldo += 1

            # LM10 => a vote for Messi
            if "lm10" in text:
                self.messi += 1

            # Track who's talking, for the live commenter feed.
            if author:
                if author in self._recent_commenters:
                    self._recent_commenters.remove(author)
                self._recent_commenters.appendleft(author)

                # Tally toward this window's "top commenter" spotlight.
                self._window_counts[author] = self._window_counts.get(author, 0) + 1

    def reset_top_window(self) -> None:
        """Clear the top-commenter tally so a new 2-minute window can start fresh."""
        with self._lock:
            self._window_counts = {}

    def snapshot(self) -> dict:
        """Return vote counts, win-probability percentages, and commenter info as a dict."""
        with self._lock:
            ronaldo, messi = self.ronaldo, self.messi
            recent_commenters = list(self._recent_commenters)
            window_counts = dict(self._window_counts)

        total = ronaldo + messi
        if total == 0:
            # No votes yet — default to an even split so the wheel isn't biased.
            ronaldo_pct, messi_pct = 50.0, 50.0
        else:
            ronaldo_pct = round(ronaldo / total * 100, 1)
            messi_pct = round(100 - ronaldo_pct, 1)

        top_commenter = None
        if window_counts:
            name, count = max(window_counts.items(), key=lambda kv: kv[1])
            top_commenter = {"name": name, "count": count}

        return {
            "ronaldo_votes": ronaldo,
            "messi_votes": messi,
            "total_votes": total,
            "ronaldo_pct": ronaldo_pct,
            "messi_pct": messi_pct,
            "recent_commenters": recent_commenters,
            "top_commenter": top_commenter,
            "timestamp": time.time(),
        }


def chat_listener(video_id: str, counter: VoteCounter, stop_event: threading.Event) -> None:
    """
    Runs in a background thread. Continuously polls the YouTube Live Chat
    for new messages and feeds each one into the VoteCounter.
    """
    chat = pytchat.create(video_id=video_id)
    print(f"[chat] connected to live chat for video: {video_id}")

    try:
        while chat.is_alive() and not stop_event.is_set():
            for c in chat.get().sync_items():
                counter.register(c.author.name, c.message)
                print(f"[chat] {c.author.name}: {c.message}")
    except Exception as exc:
        print(f"[chat] listener stopped: {exc}")
    finally:
        chat.terminate()
        print("[chat] disconnected")


# Set of currently connected frontend clients (the HTML game, possibly more than one instance).
CONNECTED_CLIENTS = set()


async def broadcast_loop(counter: VoteCounter, interval: float) -> None:
    """Every `interval` seconds, push the latest vote split and commenter info out to all connected clients."""
    while True:
        if CONNECTED_CLIENTS:
            payload = json.dumps(counter.snapshot())
            websockets.broadcast(CONNECTED_CLIENTS, payload)
        await asyncio.sleep(interval)


async def reset_top_window_loop(counter: VoteCounter, top_window: float) -> None:
    """Every `top_window` seconds, start a fresh "top commenter" tally so a new viewer gets featured next."""
    while True:
        await asyncio.sleep(top_window)
        counter.reset_top_window()


async def handle_client(websocket) -> None:
    """Registers a new frontend connection and keeps it open until it disconnects."""
    CONNECTED_CLIENTS.add(websocket)
    print(f"[ws] client connected ({len(CONNECTED_CLIENTS)} total)")
    try:
        # This server only pushes data out; it doesn't need anything from the client,
        # but we still need to await incoming messages to detect a disconnect.
        async for _ in websocket:
            pass
    finally:
        CONNECTED_CLIENTS.discard(websocket)
        print(f"[ws] client disconnected ({len(CONNECTED_CLIENTS)} total)")


async def main(video_id: str, host: str, port: int, interval: float, top_window: float) -> None:
    counter = VoteCounter()
    stop_event = threading.Event()

    # pytchat's polling loop is blocking, so it runs on its own thread
    # while the asyncio event loop handles the WebSocket server.
    listener_thread = threading.Thread(
        target=chat_listener, args=(video_id, counter, stop_event), daemon=True
    )
    listener_thread.start()

    async with websockets.serve(handle_client, host, port):
        print(f"[ws] server running at ws://{host}:{port}")
        print(f"[ws] top-commenter spotlight resets every {top_window:.0f}s")
        print("[ws] waiting for the HTML game to connect...")
        await asyncio.gather(
            broadcast_loop(counter, interval),
            reset_top_window_loop(counter, top_window),
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Count YouTube live chat votes (CR7/LM10), track commenters, and broadcast it all over WebSocket."
    )
    parser.add_argument("--video-id", required=True, help="YouTube Live video ID (the part after v= in the URL)")
    parser.add_argument("--host", default="localhost", help="WebSocket server host (default: localhost)")
    parser.add_argument("--port", type=int, default=8765, help="WebSocket server port (default: 8765)")
    parser.add_argument("--interval", type=float, default=1.0, help="Broadcast interval in seconds (default: 1.0)")
    parser.add_argument("--top-window", type=float, default=120.0, help="Seconds per top-commenter window (default: 120 = 2 minutes)")
    args = parser.parse_args()

    try:
        asyncio.run(main(args.video_id, args.host, args.port, args.interval, args.top_window))
    except KeyboardInterrupt:
        print("\n[server] shutting down")
