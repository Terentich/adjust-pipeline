package com.github.terentich.adjust.dataloader.model;

import java.util.List;

public class IgraData {
    private final IgraHeader header;
    private final List<IgraRecord> records;

    public IgraData(IgraHeader header, List<IgraRecord> records) {
        this.header = header;
        this.records = records;
    }

    public IgraHeader getHeader() {
        return header;
    }

    public List<IgraRecord> getRecords() {
        return records;
    }
}
