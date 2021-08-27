package io.delta.standalone.data;

import java.net.URI;

/**
 * Description:
 *
 * @author : jacobzheng
 * Version: 1.0
 * Create Date Time: 2021/8/26 17:11.
 * Update Date Time:
 */
public interface CloseableFileIterator<T> extends CloseableIterator<T>{
    /**
     * get current file
     * @return current file
     */
    URI getCurrentFile();
}
