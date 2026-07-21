package com.expert.bigdata.func;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DecisionLogicTest {

    @Test
    void decideAction_boundaries() {
        assertEquals("BUY", DecisionLogic.decideAction(9));
        assertEquals("HOLD", DecisionLogic.decideAction(8));   // 边界：>8 才 BUY
        assertEquals("HOLD", DecisionLogic.decideAction(2));   // 边界：<2 才 SELL
        assertEquals("SELL", DecisionLogic.decideAction(1));
    }

    @Test
    void computeStats_emptyMatches() {
        DecisionLogic.BacktestStats stats = DecisionLogic.computeStats(List.of());
        assertEquals(0, stats.validMatches());
        assertEquals(0.0, stats.winRate());
        assertEquals(0.0, stats.maxSimilarity());
    }

    @Test
    void computeStats_filtersBySimilarityThreshold() {
        // 0.9 不算（阈值为严格大于），0.95 和 0.92 算；其中一条 histReturn>0
        DecisionLogic.BacktestStats stats = DecisionLogic.computeStats(List.of(
                new DecisionLogic.BacktestMatch(0.90f, 5.0f),
                new DecisionLogic.BacktestMatch(0.95f, 0.02f),
                new DecisionLogic.BacktestMatch(0.92f, -0.01f)));
        assertEquals(2, stats.validMatches());
        assertEquals(0.5, stats.winRate());
        assertEquals(0.95, stats.maxSimilarity(), 1e-6);
    }

    @Test
    void buildFilterExpr_format() {
        assertEquals("sentiment_score == 9 && rsi_14 >= 55.00 && is_settled == true",
                DecisionLogic.buildFilterExpr(9, 60.0));
    }

    @Test
    void signalId_format() {
        assertEquals("evt-123:SELL", DecisionLogic.signalId("evt-123", "SELL"));
    }
}
