package org.noahsark.process.beans;

/**
 * 用户请求类
 *
 * @author zhangxt
 * @date 2022/09/14 10:43
 **/
public class UserRequest {

    // 用户id
    private String userId;

    // 操作类型
    private String opertationType;

    // 时间戳（S）
    private Long timestamp;

    public UserRequest() {
    }

    public UserRequest(String userId, String opertationType, Long timestamp) {
        this.userId = userId;
        this.opertationType = opertationType;
        this.timestamp = timestamp;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getOpertationType() {
        return opertationType;
    }

    public void setOpertationType(String opertationType) {
        this.opertationType = opertationType;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "UserRequest{" +
                "userId='" + userId + '\'' +
                ", opertationType='" + opertationType + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
