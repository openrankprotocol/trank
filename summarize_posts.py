#!/usr/bin/env python3
import os
import psycopg2
import argparse
import json
import time
import logging
from typing import List, Optional, Dict, Any

from concurrent.futures import ThreadPoolExecutor, as_completed
from openai import OpenAI

logger = logging.getLogger(__name__)

model_name = "gpt-4.1-mini"

def get_top_messages(
    db_url: str,
    channel_id: int,
    run_id: Optional[int],
    limit: int,
) -> List[tuple]:
    """
    Fetch top messages for a single channel.
    If run_id is None, do not filter scores by run_id.
    """
    logger.debug(
        "Fetching top messages for channel_id=%s, run_id=%s, limit=%s",
        channel_id,
        run_id,
        limit,
    )

    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            run_id_filter = "AND s.run_id = %s" if run_id is not None else ""

            query = f"""
                WITH interaction AS (
                    SELECT
                        m.channel_id,
                        parent.id AS message_id,
                        m.from_id AS user_id,
                        'reply' AS interaction_type
                    FROM trank.messages m
                    JOIN trank.messages parent
                        ON parent.channel_id = m.channel_id
                       AND m.reply_to_msg_id = parent.id
                    WHERE m.channel_id = %s
                    UNION ALL
                    SELECT
                        r.channel_id,
                        r.message_id,
                        r.user_id,
                        'reaction' AS interaction_type
                    FROM trank.message_reactions r
                    WHERE r.channel_id = %s
                ),
                interaction_scores AS (
                    SELECT
                        i.message_id,
                        i.interaction_type,
                        s.user_id,
                        s.value AS user_score
                    FROM interaction i
                    JOIN trank.scores s
                        ON s.channel_id = i.channel_id
                       AND s.user_id = i.user_id
                       {run_id_filter}
                ),
                weighted AS (
                    SELECT
                        message_id,
                        SUM(
                            user_score *
                            CASE interaction_type
                                WHEN 'reply' THEN 10
                                WHEN 'reaction' THEN 20
                            END
                        ) AS score
                    FROM interaction_scores
                    GROUP BY message_id
                )
                SELECT
                    m.id,
                    m.channel_id,
                    m.date,
                    m.from_id,
                    m.message,
                    COALESCE(w.score, 0) AS score
                FROM trank.messages m
                LEFT JOIN weighted w
                  ON w.message_id = m.id
                WHERE m.channel_id = %s
                ORDER BY score DESC
                LIMIT %s
            """

            if run_id is not None:
                params = (channel_id, channel_id, run_id, channel_id, limit)
            else:
                params = (channel_id, channel_id, channel_id, limit)

            cur.execute(query, params)
            rows = cur.fetchall()
            logger.debug(
                "Fetched %d messages for channel_id=%s", len(rows), channel_id
            )
            return rows
    finally:
        conn.close()


def summarize_with_openai(
    messages: List[str],
    client: OpenAI,
    max_retries: int = 3,
    base_delay: float = 1.0,
) -> Dict[str, Any]:
    """
    Summarize messages using a shared OpenAI client.
    Retries if the response is not valid JSON or if OpenAI call fails.
    """
    logger.debug("Summarizing %d messages with OpenAI", len(messages))

    messages_json = json.dumps(messages, ensure_ascii=False)
    prompt = (
        "Here are top messages as a JSON array:\n"
        f"{messages_json}\n\n"
        "Return only a JSON object with the following fields:\n"
        "topic: a 1-3 word description of the main topic\n"
        "few_words: 1-7 words summarizing the content\n"
        "one_sentence: one very concise sentence summarizing the discussion"
    )

    last_error: Optional[Exception] = None

    for attempt in range(max_retries):
        try:
            logger.debug("OpenAI summarize attempt %d/%d", attempt + 1, max_retries)
            resp = client.responses.create(
                model=model_name,
                input=prompt,
                temperature=0.1,
                instructions=(
                    "You summarize discussions into a short JSON object "
                    "and return JSON not markdown."
                ),
            )
            content = resp.output_text.strip()
            logger.debug("OpenAI raw response text: %s", content[:500])
            return json.loads(content)
        except Exception as e:
            last_error = e
            logger.warning(
                "OpenAI summarize failed on attempt %d/%d: %s",
                attempt + 1,
                max_retries,
                e,
            )
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                logger.debug("Sleeping for %s seconds before retry", delay)
                time.sleep(delay)

    logger.error(
        "OpenAI summarization failed after %d attempts: %s",
        max_retries,
        last_error,
    )
    return {
        "topic": None,
        "few_words": None,
        "one_sentence": None,
        "error": f"Failed to summarize after {max_retries} attempts: {last_error}",
    }


def process_channel(
    db_url: str,
    channel_id: int,
    run_id: Optional[int],
    limit: int,
    client: OpenAI,
    max_retries: int = 2,
) -> Dict[str, Any]:
    """
    Process a single channel: fetch top messages and summarize.
    Retries the whole processing a few times on failure.
    """
    logger.info("Processing channel_id=%s", channel_id)
    last_error: Optional[Exception] = None

    for attempt in range(max_retries):
        try:
            logger.debug(
                "Channel %s processing attempt %d/%d",
                channel_id,
                attempt + 1,
                max_retries,
            )
            rows = get_top_messages(db_url, channel_id, run_id, limit)
            messages: List[Dict[str, Any]] = []
            for r in rows:
                messages.append(
                    {
                        "id": r[0],
                        "channel_id": r[1],
                        "date": (
                            r[2].isoformat()
                            if hasattr(r[2], "isoformat")
                            else str(r[2])
                        ),
                        "from_id": r[3],
                        "message": r[4],
                        "score": r[5],
                    }
                )

            messages_to_summarize = [x["message"] for x in messages]
            summary = summarize_with_openai(messages_to_summarize, client=client)

            logger.info("Finished processing channel_id=%s", channel_id)
            return {
                "channel": channel_id,
                "summary": summary,
            }
        except Exception as e:
            last_error = e
            logger.warning(
                "Failed to process channel_id=%s on attempt %d/%d: %s",
                channel_id,
                attempt + 1,
                max_retries,
                e,
            )
            if attempt < max_retries - 1:
                delay = 1.0 * (2 ** attempt)
                logger.debug(
                    "Channel %s retrying after %s seconds", channel_id, delay
                )
                time.sleep(delay)

    logger.error(
        "Failed to process channel_id=%s after %d attempts: %s",
        channel_id,
        max_retries,
        last_error,
    )
    return {
        "channel": channel_id,
        "error": f"Failed to process channel after {max_retries} attempts: {last_error}",
    }


def save_summaries(db_url, results, run_id, limit, model):
    conn = psycopg2.connect(db_url)
    try:
        with conn:
            with conn.cursor() as cur:
                for item in results:
                    if "summary" not in item:
                        continue

                    channel_id = str(item["channel"])
                    s = item["summary"] or {}
                    topic = s.get("topic")
                    few_words = s.get("few_words")
                    one_sentence = s.get("one_sentence")
                    error = s.get("error")

                    if run_id is None:
                        cur.execute(
                            """
                            INSERT INTO trank.channel_summaries (
                                channel_id,
                                run_id,
                                messages_limit,
                                summary,
                                topic,
                                few_words,
                                one_sentence,
                                error,
                                model
                            )
                            VALUES (
                                %s,
                                NULL,
                                %s,
                                %s::jsonb,
                                %s,
                                %s,
                                %s,
                                %s,
                                %s
                            )
                            ON CONFLICT (channel_id) WHERE run_id IS NULL DO UPDATE SET
                                messages_limit = EXCLUDED.messages_limit,
                                summary = EXCLUDED.summary,
                                topic = EXCLUDED.topic,
                                few_words = EXCLUDED.few_words,
                                one_sentence = EXCLUDED.one_sentence,
                                error = EXCLUDED.error,
                                model = EXCLUDED.model,
                                created_at = NOW();
                            """,
                            (
                                channel_id,
                                limit,
                                json.dumps(s, ensure_ascii=False),
                                topic,
                                few_words,
                                one_sentence,
                                error,
                                model,
                            ),
                        )
                    else:
                        cur.execute(
                            """
                            INSERT INTO trank.channel_summaries (
                                channel_id,
                                run_id,
                                messages_limit,
                                summary,
                                topic,
                                few_words,
                                one_sentence,
                                error,
                                model
                            )
                            VALUES (
                                %s,
                                %s,
                                %s,
                                %s::jsonb,
                                %s,
                                %s,
                                %s,
                                %s,
                                %s
                            )
                            ON CONFLICT (channel_id, run_id) WHERE run_id IS NOT NULL DO UPDATE SET
                                messages_limit = EXCLUDED.messages_limit,
                                summary = EXCLUDED.summary,
                                topic = EXCLUDED.topic,
                                few_words = EXCLUDED.few_words,
                                one_sentence = EXCLUDED.one_sentence,
                                error = EXCLUDED.error,
                                model = EXCLUDED.model,
                                created_at = NOW();
                            """,
                            (
                                channel_id,
                                str(run_id),
                                limit,
                                json.dumps(s, ensure_ascii=False),
                                topic,
                                few_words,
                                one_sentence,
                                error,
                                model,
                            ),
                        )
    finally:
        conn.close()


def process_channels_concurrently(
    db_url: str,
    channel_ids: List[int],
    run_id: Optional[int],
    limit: int,
    client: OpenAI,
    max_workers: int = 10,
) -> List[Dict[str, Any]]:
    """
    Process multiple channels concurrently in a thread pool.
    Uses a shared OpenAI client instance across all workers.
    """
    logger.info(
        "Processing %d channels concurrently with max_workers=%d",
        len(channel_ids),
        max_workers,
    )
    results: List[Dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_channel = {
            executor.submit(
                process_channel,
                db_url,
                channel_id,
                run_id,
                limit,
                client,
            ): channel_id
            for channel_id in channel_ids
        }

        for future in as_completed(future_to_channel):
            channel_id = future_to_channel[future]
            try:
                res = future.result()
            except Exception as e:
                logger.exception(
                    "Unhandled error while processing channel_id=%s: %s",
                    channel_id,
                    e,
                )
                res = {
                    "channel": channel_id,
                    "error": str(e),
                }
            results.append(res)

    logger.info("Finished processing all channels, uploading to db")
    save_summaries(
        db_url=db_url,
        results=results,
        run_id=run_id,
        limit=limit,
        model=model_name,
    )
    logger.info("Finished uploading to db")
    return results


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--channel-id",
        type=int,
        action="append",
        dest="channel_ids",
        required=True,
        help="Channel ID (repeat this flag for multiple channels)",
    )
    parser.add_argument("--run-id", type=int, required=False)
    parser.add_argument("--limit", type=int, default=10)
    args = parser.parse_args()

    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL environment variable is required")

    client = OpenAI()

    results = process_channels_concurrently(
        db_url=url,
        channel_ids=args.channel_ids,
        run_id=args.run_id,
        limit=args.limit,
        client=client,
        max_workers=5,
    )


if __name__ == "__main__":
    main()
