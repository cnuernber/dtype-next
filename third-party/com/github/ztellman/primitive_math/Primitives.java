package com.github.ztellman.primitive_math;

public class Primitives {

    public static byte toByte(long n) {
        return (byte) n;
    }

    public static short toShort(long n) {
        return (short) n;
    }

    public static int toInteger(long n) {
        return (int) n;
    }

    public static float toFloat(double n) {
        return (float) n;
    }

    public static short reverseShort(long n) {
        return (short) (((short) n << 8)
                        | ((char) n >>> 8));
    }

    public static int reverseInteger(long n) {
        int x = (int) n;
        return ((x << 24)
                | ((x & 0x0000ff00) <<  8)
                | ((x & 0x00ff0000) >>> 8)
                | (x >>> 24));
    }

    public static long reverseLong(long n) {
        return (((long) reverseInteger(n) << 32)
                | ((long) reverseInteger((n >>> 32)) & 0xffffffffL));
    }

    ////

    public static boolean isTrue(boolean x) {
        return x == true;
    }

    public static boolean isFalse(boolean x) {
        return x == false;
    }

    public static boolean and(boolean a, boolean b) {
        return a && b;
    }

    public static boolean or(boolean a, boolean b) {
        return a || b;
    }

    public static boolean not(boolean a) {
        return !a;
    }

    public static boolean xor(boolean a, boolean b) {
        return (a || b) && !(a && b);
    }


    ////

    public static long bitAnd(long a, long b) {
        return a & b;
    }

    public static long bitOr(long a, long b) {
        return a | b;
    }

    public static long bitXor(long a, long b) {
        return a ^ b;
    }

    public static long bitNot(long a) {
        return ~a;
    }

    public static long shiftLeft(long a, long n) {
        return a << n;
    }

    public static long shiftRight(long a, long n) {
        return a >> n;
    }

    public static long unsignedShiftRight(long a, long n) {
        return a >>> n;
    }

    public static int unsignedShiftRight(int a, long n) {
        return a >>> n;
    }

    ////

    public static boolean lt(long a, long b) {
        return a < b;
    }

    public static boolean lt(float a, float b) {
        return a < b;
    }

    public static boolean lt(double a, double b) {
        return a < b;
    }

    public static boolean lte(double a, double b) {
        return a <= b;
    }

    public static boolean lte(float a, float b) {
        return a <= b;
    }

    public static boolean lte(long a, long b) {
        return a <= b;
    }

    public static boolean gt(long a, long b) {
        return a > b;
    }

    public static boolean gt(float a, float b) {
        return a > b;
    }

    public static boolean gt(double a, double b) {
        return a > b;
    }

    public static boolean gte(long a, long b) {
        return a >= b;
    }

    public static boolean gte(float a, float b) {
        return a >= b;
    }

    public static boolean gte(double a, double b) {
        return a >= b;
    }

    public static boolean eq(long a, long b) {
        return a == b;
    }

    public static boolean eq(float a, float b) {
        return a == b;
    }

    public static boolean eq(double a, double b) {
        return a == b;
    }

    public static boolean neq(long a, long b) {
        return a != b;
    }

    public static boolean neq(float a, float b) {
        return a != b;
    }

    public static boolean neq(double a, double b) {
        return a != b;
    }

    ////

    public static long rem(long n, long div) {
        return n % div;
    }

    public static long inc(long n) {
        return n + 1;
    }

    public static float inc(float n) {
        return n + 1.0f;
    }

    public static double inc(double n) {
        return n + 1.0;
    }

    public static long dec(long n) {
        return n - 1;
    }

    public static float dec(float n) {
        return n - 1.0f;
    }

    public static double dec(double n) {
        return n - 1.0;
    }

    public static boolean isZero(long n) {
        return n == 0;
    }

    public static boolean isZero(float n) {
        return n == 0.0f;
    }

    public static boolean isZero(double n) {
        return n == 0.0;
    }

    public static long add(long a, long b) {
        return a + b;
    }

    public static float add(float a, float b) {
        return a + b;
    }

    public static double add(double a, double b) {
        return a + b;
    }

    public static long subtract(long a, long b) {
        return a - b;
    }

    public static float subtract(float a, float b) {
        return a - b;
    }

    public static double subtract(double a, double b) {
        return a - b;
    }

    public static long negate(long n) {
        return -n;
    }

    public static float negate(float n) {
        return -n;
    }

    public static double negate(double n) {
        return -n;
    }

    public static long multiply(long a, long b) {
        return a * b;
    }

    public static float multiply(float a, float b) {
        return a * b;
    }

    public static double multiply(double a, double b) {
        return a * b;
    }

    public static long divide(long a, long b) {
        return a / b;
    }

    public static float divide(float a, float b) {
        return a / b;
    }

    public static double divide(double a, double b) {
        return a / b;
    }

    ;;;

    public static long max(long a, long b) {
        return a < b ? b : a;
    }

    public static long min(long a, long b) {
        return a > b ? b : a;
    }

    public static float max(float a, float b) {
        return a < b ? b : a;
    }

    public static float min(float a, float b) {
        return a > b ? b : a;
    }

    public static double max(double a, double b) {
        return a < b ? b : a;
    }

    public static double min(double a, double b) {
        return a > b ? b : a;
    }

}
