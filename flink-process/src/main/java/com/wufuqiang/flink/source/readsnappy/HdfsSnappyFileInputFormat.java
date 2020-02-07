package com.wufuqiang.flink.source.readsnappy;

import org.apache.flink.api.common.io.InputStreamFSInputWrapper;
import org.apache.flink.api.common.io.compression.InflaterInputStreamFactory;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;

public class HdfsSnappyFileInputFormat extends TextInputFormat {

    public HdfsSnappyFileInputFormat(Path filePath) {
        super(filePath);
    }

    protected boolean testForUnsplittable(FileStatus pathFile) {
        return true;
    }

    protected FSDataInputStream decorateInputStream(FSDataInputStream inputStream, FileInputSplit fileSplit) throws Throwable {
        // Wrap stream in a extracting (decompressing) stream if file ends with a known compression file extension.
        InflaterInputStreamFactory<?> inflaterInputStreamFactory = SnappyHadoopInputStreamFactory.getInstance();
        if (inflaterInputStreamFactory != null) {
            return new InputStreamFSInputWrapper(inflaterInputStreamFactory.create(stream));
        }

        return inputStream;
    }

}










