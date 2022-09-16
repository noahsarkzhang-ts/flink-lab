package org.noahsark.process.beans;

/**
 * 统计一次会话用户请求次数
 *
 * @author zhangxt
 * @date 2022/09/14 10:47
 **/
public class CountWithTimestamp {

    // 用户id
    private String userId;

    // 统计次数
    private Long count = 0L;

    // 上次修改时间戳
    private Long lastModified;

    public CountWithTimestamp() {
    }

    public CountWithTimestamp(String userId, Long count, Long lastModified) {
        this.userId = userId;
        this.count = count;
        this.lastModified = lastModified;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public Long getLastModified() {
        return lastModified;
    }

    public void setLastModified(Long lastModified) {
        this.lastModified = lastModified;
    }

    @Override
    public String toString() {
        return "CountWithTimestamp{" +
                "userId='" + userId + '\'' +
                ", count=" + count +
                ", lastModified=" + lastModified +
                '}';
    }
}
