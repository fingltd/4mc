package com.fing.compression.fourmc;


import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class TestFourMcBlockIndex extends TestCase {

    protected void setUp() throws Exception {
    }

    protected void tearDown() throws Exception {
    }

    public static Test suite() {
        return new TestSuite(TestFourMcBlockIndex.class);
    }

    public void testCreate() {
        FourMcBlockIndex empty = new FourMcBlockIndex();
        assertTrue(empty.isEmpty());

        FourMcBlockIndex idx = new FourMcBlockIndex(16);
        assertTrue(!idx.isEmpty());
    }

    public void testSet() {
        FourMcBlockIndex idx = new FourMcBlockIndex(16);
        assertTrue(!idx.isEmpty());
        for (int i=0; i<idx.getNumberOfBlocks(); ++i) {
            idx.set(i, i*10);
        }

        for (int i=0; i<16; ++i) {
            assertTrue(idx.getPosition(i)==i*10);
        }

    }

    public void testFindNextPosition() {
        FourMcBlockIndex idx = new FourMcBlockIndex(4);
        idx.set(0, 100);
        idx.set(1, 200);
        idx.set(2, 300);
        idx.set(3, 400);

        assertTrue(idx.findNextPosition(100)==100);
        assertTrue(idx.findNextPosition(110)==200);
        assertTrue(idx.findNextPosition(210)==300);

    }

    public void testFindBelongingBlockIndex() {
        FourMcBlockIndex idx = new FourMcBlockIndex(4);
        idx.set(0, 100);
        idx.set(1, 200);
        idx.set(2, 300);
        idx.set(3, 400);

        assertTrue(idx.findBelongingBlockIndex(50)==FourMcBlockIndex.NOT_FOUND);
        assertTrue(idx.findBelongingBlockIndex(100)==0);
        assertTrue(idx.findBelongingBlockIndex(110)==0);
        assertTrue(idx.findBelongingBlockIndex(210)==1);
        assertTrue(idx.findBelongingBlockIndex(300)==2);
        assertTrue(idx.findBelongingBlockIndex(350)==2);
        assertTrue(idx.findBelongingBlockIndex(400)==3);
        assertTrue(idx.findBelongingBlockIndex(450)==3);

    }

    public void testAlignSlice() {
        FourMcBlockIndex idx = new FourMcBlockIndex(4);
        idx.set(0, 100);
        idx.set(1, 200);
        idx.set(2, 300);
        idx.set(3, 400);

        assertTrue(idx.alignSliceStartToIndex(0, 350)==0);
        assertTrue(idx.alignSliceStartToIndex(100, 350)==100);

        assertTrue(idx.alignSliceEndToIndex(350, 550)==400);
        assertTrue(idx.alignSliceEndToIndex(250, 550)==300);
    }

}
