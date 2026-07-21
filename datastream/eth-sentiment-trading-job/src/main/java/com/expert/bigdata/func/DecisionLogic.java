package com.expert.bigdata.func;

import java.util.List;

/**
 * 交易决策的纯逻辑，与 Flink/Milvus 运行时解耦以便单元测试。
 * 阈值与 EthBacktestDecisionFunction 原实现保持一致。
 */
public final class DecisionLogic {

    public static final double SIMILARITY_THRESHOLD = 0.9;

    public record BacktestMatch(float similarity, float histReturn) {}

    public record BacktestStats(int validMatches, double winRate, double maxSimilarity) {}

    private DecisionLogic() {}

    public static String decideAction(long sentimentScore) {
        if (sentimentScore > 8) {
            return "BUY";
        }
        if (sentimentScore < 2) {
            return "SELL";
        }
        return "HOLD";
    }

    public static BacktestStats computeStats(List<BacktestMatch> matches) {
        int valid = 0;
        double winCount = 0;
        double maxSim = 0;
        for (BacktestMatch m : matches) {
            if (m.similarity() > SIMILARITY_THRESHOLD) {
                valid++;
                maxSim = Math.max(maxSim, m.similarity());
                if (m.histReturn() > 0) {
                    winCount++;
                }
            }
        }
        return new BacktestStats(valid, valid > 0 ? winCount / valid : 0, maxSim);
    }

    public static String buildFilterExpr(long sentimentScore, double rsi14) {
        return String.format("sentiment_score == %d && rsi_14 >= %.2f && is_settled == true",
                sentimentScore, rsi14 - 5);
    }

    public static String signalId(String eventId, String action) {
        return eventId + ":" + action;
    }
}
