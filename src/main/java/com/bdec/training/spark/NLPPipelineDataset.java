package com.bdec.training.spark;

import java.io.Serializable;

public class NLPPipelineDataset implements Serializable {
    Long id;
    String text;
    Double label;

    public NLPPipelineDataset() {
    }

    public NLPPipelineDataset(Long id, String text, Double label) {
        this.id = id;
        this.text = text;
        this.label = label;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public Double getLabel() {
        return label;
    }

    public void setLabel(Double label) {
        this.label = label;
    }
}
