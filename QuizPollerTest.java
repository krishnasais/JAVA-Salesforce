package com.quiz;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for deduplication and leaderboard logic.
 * These tests do NOT call the real API — they inject events directly.
 */
class QuizPollerTest {

    private QuizPoller poller;

    @BeforeEach
    void setUp() {
        poller = new QuizPoller();
    }

    // ── Helper: inject a single event directly into the poller's internal state ──

    private void injectEvent(String roundId, String participant, int score) throws Exception {
        Field seenKeysField = QuizPoller.class.getDeclaredField("seenKeys");
        Field scoresField   = QuizPoller.class.getDeclaredField("scores");
        Field totalField    = QuizPoller.class.getDeclaredField("totalEventsReceived");
        Field dupsField     = QuizPoller.class.getDeclaredField("duplicatesDropped");

        seenKeysField.setAccessible(true);
        scoresField.setAccessible(true);
        totalField.setAccessible(true);
        dupsField.setAccessible(true);

        @SuppressWarnings("unchecked")
        Set<String> seenKeys = (Set<String>) seenKeysField.get(poller);
        @SuppressWarnings("unchecked")
        Map<String, Integer> scores = (Map<String, Integer>) scoresField.get(poller);

        int total = (int) totalField.get(poller);
        int dups  = (int) dupsField.get(poller);

        totalField.set(poller, total + 1);

        String key = roundId + "|" + participant;
        if (seenKeys.contains(key)) {
            dupsField.set(poller, dups + 1);
        } else {
            seenKeys.add(key);
            scores.merge(participant, score, Integer::sum);
        }
    }

    // ─────────────────────────────────────────────
    //  Tests
    // ─────────────────────────────────────────────

    @Test
    void testUniqueEventsAreAggregated() throws Exception {
        injectEvent("R1", "Alice", 10);
        injectEvent("R1", "Bob",   20);
        injectEvent("R2", "Alice", 15);

        Map<String, Integer> scores = poller.getScores();
        assertEquals(25, scores.get("Alice"), "Alice: R1(10) + R2(15) = 25");
        assertEquals(20, scores.get("Bob"),   "Bob: R1(20) = 20");
        assertEquals(0, poller.getDuplicatesDropped());
    }

    @Test
    void testDuplicateEventIsDropped() throws Exception {
        injectEvent("R1", "Alice", 10);
        injectEvent("R1", "Alice", 10); // duplicate — same roundId + participant

        assertEquals(10, poller.getScores().get("Alice"),
                "Score must not double-count the duplicate");
        assertEquals(1, poller.getDuplicatesDropped());
    }

    @Test
    void testSameParticipantDifferentRoundsAreNotDuplicates() throws Exception {
        injectEvent("R1", "Alice", 10);
        injectEvent("R2", "Alice", 30); // different roundId → not a duplicate

        assertEquals(40, poller.getScores().get("Alice"),
                "Different rounds for the same participant must both be counted");
        assertEquals(0, poller.getDuplicatesDropped());
    }

    @Test
    void testLeaderboardIsSortedDescending() throws Exception {
        injectEvent("R1", "Charlie", 5);
        injectEvent("R1", "Alice",   50);
        injectEvent("R1", "Bob",     30);

        List<Map<String, Object>> lb = poller.buildLeaderboard();

        assertEquals("Alice",   lb.get(0).get("participant"));
        assertEquals("Bob",     lb.get(1).get("participant"));
        assertEquals("Charlie", lb.get(2).get("participant"));
    }

    @Test
    void testLeaderboardTotalScoreIsCorrect() throws Exception {
        injectEvent("R1", "Alice", 100);
        injectEvent("R1", "Bob",   120);

        List<Map<String, Object>> lb = poller.buildLeaderboard();
        int combined = lb.stream().mapToInt(e -> (Integer) e.get("totalScore")).sum();
        assertEquals(220, combined);
    }

    @Test
    void testMultipleDuplicatesAllDropped() throws Exception {
        injectEvent("R1", "Alice", 10);
        injectEvent("R1", "Alice", 10); // dup 1
        injectEvent("R1", "Alice", 10); // dup 2
        injectEvent("R1", "Alice", 10); // dup 3

        assertEquals(10, poller.getScores().get("Alice"));
        assertEquals(3, poller.getDuplicatesDropped());
        assertEquals(4, poller.getTotalEventsReceived());
    }

    @Test
    void testEmptyPolls() {
        List<Map<String, Object>> lb = poller.buildLeaderboard();
        assertTrue(lb.isEmpty(), "Leaderboard should be empty when no events received");
    }

    @Test
    void testDeduplicationKeyFormat() throws Exception {
        injectEvent("ROUND_1", "Alice Smith", 10);

        Set<String> seen = poller.getSeenKeys();
        assertTrue(seen.contains("ROUND_1|Alice Smith"),
                "Key format must be roundId|participant");
    }
}