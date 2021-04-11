package com.basicprograms;

class parent {
        static int y = 4;
    }
class child extends parent {
    static {
        y = 5;
    }
}
public class InnerClassStatic   {
    public static void main(String[] args){
        System.out.println("y = "+child.y);
    }
}


