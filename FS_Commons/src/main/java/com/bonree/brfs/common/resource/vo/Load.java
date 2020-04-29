package com.bonree.brfs.common.resource.vo;

public class Load {
    private double min1Load;
    private double min5Load;
    private double min15Load;

    public double getMin1Load() {
        return min1Load;
    }

    public void setMin1Load(double min1Load) {
        this.min1Load = min1Load;
    }

    public double getMin5Load() {
        return min5Load;
    }

    public void setMin5Load(double min5Load) {
        this.min5Load = min5Load;
    }

    public double getMin15Load() {
        return min15Load;
    }

    public void setMin15Load(double min15Load) {
        this.min15Load = min15Load;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Load{");
        sb.append("min1Load=").append(min1Load);
        sb.append(", min5Load=").append(min5Load);
        sb.append(", min15Load=").append(min15Load);
        sb.append('}');
        return sb.toString();
    }
}
