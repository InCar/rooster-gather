package com.incarcloud.rooster.gather;

import org.junit.Test;

/**
 * GatherHostTest
 */
public class GatherHostTest {

    @Test(expected = IllegalArgumentException.class)
    public void testGatherHost() throws Exception {
        final int PORT = 7721;
        // start host
        GatherHost host = new GatherHost();
        GatherSlot slot = host.addSlot(GatherPortType.TCP, String.format("%d", PORT));
        slot.setDataParser("Any4DataParser");
        host.start();

        // setup client and send some bytes
        /*Socket skt = new Socket("127.0.0.1", PORT);
        skt.getOutputStream().write(new byte[]{ 0x01, 0x02, 0x03, 0x04, 0x05 });
        Thread.sleep(100);
        skt.close();*/
        host.stop();
    }
}