package edu.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashSet;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class PlayChepterVideoBean {
    String stt;

    String edt;

    String videoId;

    String chapterId;

    String chapterName;

    Long totalSec;

    Long playTimesCt;
    @TransientSink
    HashSet<String> userSet;

    Long watchUidCt;

    Long avgPlaySec;

    Long ts;
}
