package com.basicprograms;

class Basic1 {
    private int length, breadth, height;

    private static int w;

    public static void stSet(int W){
        w = W;
    }

    public static int stGt(){
        return w;
    }

    public void sett(int l, int b, int h) {
        length = l;
        breadth = b;
        height = h;
    }

    public int getLength() {
        return (length);
    }

    public int getBreadth() {
        return (breadth);
    }

    public int getHeight() {
        return (height);
    }
}
public class Basic{
    public static void main(String [] args){
        int l =4; int b =5; int h = 9;
        Basic1 jpg = new Basic1();
        jpg.sett(l,b,h);
        int x = jpg.getBreadth();
        int y = jpg.getHeight();
        System.out.println(y);
        System.out.println(x);

        Basic1.stSet(4);
        int z = Basic1.stGt();
        System.out.println(z);

    }
}
