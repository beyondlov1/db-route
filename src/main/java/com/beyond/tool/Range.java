package com.beyond.tool;

public class Range {
    private Integer low = Integer.MIN_VALUE;
    private Integer high = Integer.MAX_VALUE;
    private boolean includeLow;
    private boolean includeHigh;

    public static Range intersection(Range r1, Range r2) {
        int low = Integer.MIN_VALUE;
        boolean includeLow = false;
        int high = Integer.MAX_VALUE;
        boolean includeHigh = false;
        if (r1.low != null) {
            low = r1.low;
            includeLow = r1.includeLow;
        }
        if (r1.high != null) {
            high = r1.high;
            includeHigh = r1.includeHigh;
        }
        if (r2.low != null) {
            if (low < r2.low){
                low = r2.low;
                includeLow = r2.includeLow;
            }
            if (low == r2.low){
                // 有一个不包含则不包含
                if ((!includeLow) || (!r2.includeLow)){
                    includeLow = false;
                }
            }
        }
        if (r2.high != null) {
            if (high > r2.high){
                high = r2.high;
                includeHigh = r2.includeHigh;
            }
            if (high == r2.high){
                // 有一个不包含则不包含
                if ((!includeHigh) || (!r2.includeHigh)){
                    includeHigh = false;
                }
            }
        }
        return new Range(low, includeLow, high, includeHigh);
    }


    //fixme includeLow and includeHigh
    @Deprecated
    public static Range union(Range r1, Range r2) {
        int low = Integer.MIN_VALUE;
        int high = Integer.MAX_VALUE;
        if (r1.low != null) {
            low = r1.low;
        }
        if (r1.high != null) {
            high = r1.high;
        }
        if (r2.low != null) {
            low = Math.min(low, r2.low);
        }
        if (r2.high != null) {
            high = Math.max(high, r2.high);
        }
        return new Range(low, high);
    }


    public boolean isValid() {
        if (low != null && high != null) {
            return low <= high;
        }
        return true;
    }

    public Range(Integer low, Integer high) {
        this.low = low;
        this.high = high;
    }

    public Range(Integer low, boolean includeLow, Integer high, boolean includeHigh) {
        this.low = low;
        this.high = high;
        this.includeLow = includeLow;
        this.includeHigh = includeHigh;
    }

    public Integer getLow() {
        return low;
    }

    public void setLow(Integer low) {
        this.low = low;
    }

    public Integer getHigh() {
        return high;
    }

    public void setHigh(Integer high) {
        this.high = high;
    }

    public boolean isIncludeLow() {
        return includeLow;
    }

    public void setIncludeLow(boolean includeLow) {
        this.includeLow = includeLow;
    }

    public boolean isIncludeHigh() {
        return includeHigh;
    }

    public void setIncludeHigh(boolean includeHigh) {
        this.includeHigh = includeHigh;
    }

    public boolean include(int target) {
        if (low == null) {
            if (high == null) {
                return true;
            } else {
                if (includeHigh) {
                    return target <= high;
                } else {
                    return target < high;
                }
            }
        } else {
            if (high == null) {
                if (includeLow) {
                    return target >= low;
                } else {
                    return target > low;
                }
            } else {
                if (includeLow && includeHigh) {
                    return target >= low && target <= high;
                } else if (includeLow) {
                    return target >= low && target < high;
                } else if (includeHigh) {
                    return target > low && target <= high;
                } else {
                    return target > low && target < high;
                }
            }
        }
    }
}