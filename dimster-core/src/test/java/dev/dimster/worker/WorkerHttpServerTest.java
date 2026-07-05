package dev.dimster.worker;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

/**
 * Covers HTTP-server edge cases that should remain local to WorkerHttpServer.
 * The worker endpoints share one response wrapper, so validating its
 * disconnect classifier protects noisy health/stat polling without needing a
 * broker-backed LocalWorker.
 */
public class WorkerHttpServerTest {

    @Test
    public void classifiesBrokenPipeAsClientDisconnect() {
        assertTrue(WorkerHttpServer.isClientDisconnect(new IOException("Broken pipe")));
    }

    @Test
    public void classifiesConnectionResetByPeerAsClientDisconnect() {
        assertTrue(WorkerHttpServer.isClientDisconnect(new IOException("Connection reset by peer")));
    }

    @Test
    public void classifiesConnectionResetAsClientDisconnect() {
        assertTrue(WorkerHttpServer.isClientDisconnect(new IOException("Connection reset")));
    }

    @Test
    public void searchesCausesForWrappedClientDisconnect() {
        RuntimeException wrapped = new RuntimeException(
                "write failed",
                new IOException("Connection reset by peer"));

        assertTrue(WorkerHttpServer.isClientDisconnect(wrapped));
    }

    @Test
    public void searchesSuppressedExceptionsFromResponseClose() {
        IOException writeFailure = new IOException("insufficient bytes written to stream");
        writeFailure.addSuppressed(new IOException("Connection reset by peer"));

        assertTrue(WorkerHttpServer.isClientDisconnect(writeFailure));
    }

    @Test
    public void rejectsUnrelatedIoFailures() {
        assertFalse(WorkerHttpServer.isClientDisconnect(new IOException("disk write failed")));
    }
}
