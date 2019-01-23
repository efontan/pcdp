package edu.coursera.distributed;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * A basic and very limited implementation of a file server that responds to GET
 * requests from HTTP clients.
 */
public final class FileServer {

    public static final String GET_METHOD = "GET";
    public static final String SPACE_SEPARATOR = " ";
    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(5);


    /**
     * Main entrypoint for the basic file server.
     *
     * @param socket Provided socket to accept connections on.
     * @param fs     A proxy filesystem to serve files from. See the PCDPFilesystem
     *               class for more detailed documentation of its usage.
     * @param ncores The number of cores that are available to your
     *               multi-threaded file server. Using this argument is entirely
     *               optional. You are free to use this information to change
     *               how you create your threads, or ignore it.
     * @throws IOException If an I/O error is detected on the server. This
     *                     should be a fatal error, your file server
     *                     implementation is not expected to ever throw
     *                     IOExceptions during normal operation.
     */
    public void run(final ServerSocket socket, final PCDPFilesystem fs,
                    final int ncores) throws IOException {
        /*
         * Enter a spin loop for handling client requests to the provided
         * ServerSocket object.
         */
        while (true) {

            // TODO 1) Use socket.accept to get a Socket object
            final Socket request = socket.accept();
            Thread worker = new Thread(
                    () -> {
                        try {
                            String firstLine = FileServer.this.extractRequestFirstLine(request);
                            if (FileServer.this.isGetRequest(firstLine)) {
                                String filePath = FileServer.this.extractRequestFilePath(firstLine);

                                Optional<String> fileContent = FileServer.this.readFileContent(fs, filePath);
                                if (fileContent.isPresent()) {
                                    FileServer.this.printSuccessResponse(request, fileContent.get());
                                } else {
                                    FileServer.this.printNotFoundResponse(request);
                                }
                            }
                        } catch (IOException io) {
                            throw new RuntimeException(io);
                        }
                    }
            );
            executor.execute(worker);
            worker.start();
        }
    }

    private Optional<String> readFileContent(PCDPFilesystem fs, String filePath) {
        String fileConent = fs.readFile(new PCDPPath(filePath));
        return Optional.ofNullable(fileConent);
    }

    private boolean isGetRequest(String firstLine) {
        return firstLine.startsWith(GET_METHOD);
    }

    private String extractRequestFirstLine(Socket request) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(request.getInputStream()));
        String firstLine = br.readLine();
        assert firstLine != null;
        return firstLine;
    }

    private String extractRequestFilePath(String firstLine) {
        String[] firstLineParts = firstLine.split(SPACE_SEPARATOR);
        assert firstLineParts.length > 1;
        return firstLineParts[1];
    }

    private void printSuccessResponse(Socket request, String fileContent)
            throws IOException {
        StringBuilder response = new StringBuilder();

        response.append("HTTP/1.0 200 OK\r\n");
        response.append("Server: FileServer\r\n");
        response.append("\r\n");
        response.append(fileContent);
        response.append("\r\n");

        this.writeResponse(request, response.toString());
    }

    private void printNotFoundResponse(Socket request)
            throws IOException {
        StringBuilder response = new StringBuilder();

        response.append("HTTP/1.0 404 Not Found\r\n");
        response.append("Server: FileServer\r\n");
        response.append("\r\n");

        this.writeResponse(request, response.toString());
    }

    private void writeResponse(Socket socket, String response)
            throws IOException {
        OutputStream out = socket.getOutputStream();
        PrintStream ps = new PrintStream(out);

        ps.println(response);
        ps.close();
    }

}
