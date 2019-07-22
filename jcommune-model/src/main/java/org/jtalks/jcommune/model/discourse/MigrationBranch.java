package org.jtalks.jcommune.model.discourse;

import org.jtalks.jcommune.model.entity.Branch;

public class MigrationBranch extends Branch {
    private int position = -1;
    private int latestTopicId = -1;
    private int topicInBranchCount = -1;

    public MigrationBranch(String name, String description) {
        super(name, description);
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public int getLatestTopicId() {
        return latestTopicId;
    }

    public void setLatestTopicId(int latestTopicId) {
        this.latestTopicId = latestTopicId;
    }

    public int getTopicInBranchCount() {
        return topicInBranchCount;
    }

    public void setTopicInBranchCount(int topicInBranchCount) {
        this.topicInBranchCount = topicInBranchCount;
    }
}
