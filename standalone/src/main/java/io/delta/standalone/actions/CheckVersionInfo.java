package io.delta.standalone.actions;

/**
 * @Author liangyu
 * @create 2021/6/7 5:50 下午
 * @Description
 */
public class CheckVersionInfo {

    private Long earliestVersion;

    private Long latestVersion;

    private Boolean versionExist;


    private CheckVersionInfo(){
    }

    public CheckVersionInfo(Long earliestVersion, Long latestVersion, Boolean versionExist){
        this.earliestVersion = earliestVersion;
        this.latestVersion = latestVersion;
        this.versionExist = versionExist;
    }

    public Long getEarliestVersion() {
        return earliestVersion;
    }

    public Long getLatestVersion() {
        return latestVersion;
    }

    public Boolean getVersionExist() {
        return versionExist;
    }

}
