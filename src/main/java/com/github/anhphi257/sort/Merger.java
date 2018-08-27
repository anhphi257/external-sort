package com.github.anhphi257.sort;

import java.io.File;
import java.util.List;

/**
 * Created by phiha on 27/08/2018.
 */
public class Merger implements Runnable {
    private List<File> files;
    private File out;

    public Merger(List<File> files, File out) {
        this.files = files;
        this.out = out;
    }


    @Override
    public void run() {

    }
}
