package com.quiz;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * QuizPoller — polls the validator API 10 times, deduplicates events,
 * aggregates scores, builds a leaderboard, and submits once.
 *
 * Deduplication key: roundId + "|" + participant
 * A mandatory 5-second delay is observed between each poll.
 */
public class QuizPoller {

    private static final String BASE_URL  = "https://devapigw.vidalhealthtpa.com/srm-quiz-task";
    private static final String REG_NO    = "2024CS101";
    private static final int    TOTAL_POLLS   = 10;
    private static final long   POLL_DELAY_MS = 5_000L;

    private final HttpClient     httpClient;
    private final ObjectMapper   mapper;

    // Deduplication: roundId|participant → already seen
    private final Set<String>         seenKeys  = new LinkedHashSet<>();
    // Aggregation: participant → totalScore
    private final Map<String, Integer> scores    = new LinkedHashMap<>();

    // Diagnostics
    private int totalEventsReceived = 0;
    private int duplicatesDropped   = 0;

    public QuizPoller() {
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        this.mapper = new ObjectMapper();
    }

    // ─────────────────────────────────────────────
    //  Entry point
    // ─────────────────────────────────────────────

    public static void main(String[] args) throws Exception {
        QuizPoller poller = new QuizPoller();
        poller.run();
    }

    public void run() throws Exception {
        System.out.println("=".repeat(60));
        System.out.println("  Quiz Leaderboard — Polling Started");
        System.out.println("  Registration No : " + REG_NO);
        System.out.println("=".repeat(60));

        // Step 1–2: Poll 10 times and collect responses
        for (int poll = 0; poll < TOTAL_POLLS; poll++) {
            pollOnce(poll);
            if (poll < TOTAL_POLLS - 1) {
                System.out.printf("  [→] Waiting %d seconds before next poll...%n%n", POLL_DELAY_MS / 1000);
                Thread.sleep(POLL_DELAY_MS);
            }
        }

        // Step 5–6: Build leaderboard and print summary
        List<Map<String, Object>> leaderboard = buildLeaderboard();
        printSummary(leaderboard);

        // Step 7: Submit once
        submitLeaderboard(leaderboard);
    }

    // ─────────────────────────────────────────────
    //  Polling
    // ─────────────────────────────────────────────

    private void pollOnce(int pollIndex) throws IOException, InterruptedException {
        String url = String.format("%s/quiz/messages?regNo=%s&poll=%d", BASE_URL, REG_NO, pollIndex);
        System.out.printf("[Poll %d/%d] GET %s%n", pollIndex + 1, TOTAL_POLLS, url);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(15))
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            System.out.printf("  [!] HTTP %d — skipping poll %d%n", response.statusCode(), pollIndex);
            return;
        }

        JsonNode root   = mapper.readTree(response.body());
        JsonNode events = root.path("events");

        if (!events.isArray()) {
            System.out.println("  [!] No events array in response — skipping.");
            return;
        }

        int newInPoll = 0, dupInPoll = 0;

        // Step 3–4: Deduplicate and aggregate
        for (JsonNode event : events) {
            String roundId     = event.path("roundId").asText();
            String participant = event.path("participant").asText();
            int    score       = event.path("score").asInt();

            totalEventsReceived++;
            String key = roundId + "|" + participant;

            if (seenKeys.contains(key)) {
                // Duplicate — ignore
                duplicatesDropped++;
                dupInPoll++;
                System.out.printf("  [DUP] %-12s  round=%-4s  score=%-4d  → ignored%n",
                        participant, roundId, score);
            } else {
                // New unique event — aggregate
                seenKeys.add(key);
                scores.merge(participant, score, Integer::sum);
                newInPoll++;
                System.out.printf("  [NEW] %-12s  round=%-4s  score=%-4d  → accepted%n",
                        participant, roundId, score);
            }
        }

        System.out.printf("  Summary: %d new, %d duplicate(s)%n", newInPoll, dupInPoll);
    }

    // ─────────────────────────────────────────────
    //  Leaderboard construction
    // ─────────────────────────────────────────────

    /**
     * Returns participants sorted by totalScore descending.
     * Each entry: {"participant": "Alice", "totalScore": 100}
     */
    public List<Map<String, Object>> buildLeaderboard() {
        return scores.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue(Comparator.reverseOrder()))
                .map(e -> {
                    Map<String, Object> entry = new LinkedHashMap<>();
                    entry.put("participant", e.getKey());
                    entry.put("totalScore",  e.getValue());
                    return entry;
                })
                .collect(Collectors.toList());
    }

    // ─────────────────────────────────────────────
    //  Submission
    // ─────────────────────────────────────────────

    private void submitLeaderboard(List<Map<String, Object>> leaderboard)
            throws IOException, InterruptedException {

        String url = BASE_URL + "/quiz/submit";

        ObjectNode payload = mapper.createObjectNode();
        payload.put("regNo", REG_NO);
        ArrayNode lb = payload.putArray("leaderboard");
        for (Map<String, Object> entry : leaderboard) {
            ObjectNode node = lb.addObject();
            node.put("participant", (String)  entry.get("participant"));
            node.put("totalScore",  (Integer) entry.get("totalScore"));
        }

        String body = mapper.writeValueAsString(payload);
        System.out.println("\n[Submit] POST " + url);
        System.out.println("  Payload: " + body);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(15))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        System.out.println("\n" + "=".repeat(60));
        System.out.println("  SUBMISSION RESULT");
        System.out.println("=".repeat(60));
        System.out.println("  HTTP Status     : " + response.statusCode());

        JsonNode result = mapper.readTree(response.body());
        System.out.println("  isCorrect       : " + result.path("isCorrect").asBoolean());
        System.out.println("  isIdempotent    : " + result.path("isIdempotent").asBoolean());
        System.out.println("  submittedTotal  : " + result.path("submittedTotal").asInt());
        System.out.println("  expectedTotal   : " + result.path("expectedTotal").asInt());
        System.out.println("  message         : " + result.path("message").asText());
        System.out.println("=".repeat(60));
    }

    // ─────────────────────────────────────────────
    //  Summary print
    // ─────────────────────────────────────────────

    private void printSummary(List<Map<String, Object>> leaderboard) {
        int combinedScore = leaderboard.stream()
                .mapToInt(e -> (Integer) e.get("totalScore"))
                .sum();

        System.out.println("\n" + "=".repeat(60));
        System.out.println("  LEADERBOARD");
        System.out.println("=".repeat(60));
        System.out.printf("  %-4s  %-20s  %s%n", "Rank", "Participant", "Total Score");
        System.out.println("  " + "-".repeat(36));
        int rank = 1;
        for (Map<String, Object> entry : leaderboard) {
            System.out.printf("  %-4d  %-20s  %d%n",
                    rank++, entry.get("participant"), entry.get("totalScore"));
        }
        System.out.println("  " + "-".repeat(36));
        System.out.printf("  %-4s  %-20s  %d%n", "", "COMBINED TOTAL", combinedScore);
        System.out.println();
        System.out.println("  Events received : " + totalEventsReceived);
        System.out.println("  Duplicates      : " + duplicatesDropped);
        System.out.println("  Unique events   : " + (totalEventsReceived - duplicatesDropped));
        System.out.println("=".repeat(60));
    }

    // ─────────────────────────────────────────────
    //  Accessors (for tests)
    // ─────────────────────────────────────────────

    public Map<String, Integer> getScores()         { return Collections.unmodifiableMap(scores); }
    public Set<String>          getSeenKeys()        { return Collections.unmodifiableSet(seenKeys); }
    public int getTotalEventsReceived()              { return totalEventsReceived; }
    public int getDuplicatesDropped()                { return duplicatesDropped; }
}