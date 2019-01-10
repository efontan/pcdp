package edu.coursera.distributed;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Optional;

/**
 * A basic and very limited implementation of a file server that responds to GET
 * requests from HTTP clients.
 */
public final class FileServer {
    
    public static final String GET_METHOD = "GET";
    public static final String SPACE_SEPARATOR = " ";
    
    /**
     * Main entrypoint for the basic file server.
     *
     * @param socket Provided socket to accept connections on.
     * @param fs A proxy filesystem to serve files from. See the PCDPFilesystem
     *           class for more detailed documentation of its usage.
     * @throws IOException If an I/O error is detected on the server. This
     *                     should be a fatal error, your file server
     *                     implementation is not expected to ever throw
     *                     IOExceptions during normal operation.
     */
    public void run(final ServerSocket socket, final PCDPFilesystem fs) 
        throws IOException {
        /*
         * Enter a spin loop for handling client requests to the provided
         * ServerSocket object.
         */
        while (true) {
            
            Socket request = socket.accept();
            String firstLine = this.extractRequestFirstLine(request);
            
            if (this.isGetRequest(firstLine)) {
                String filePath = this.extractRequestFilePath(firstLine);
    
                Optional<String> fileContent = this.readFileContent(fs, filePath);
                if (fileContent.isPresent()) {
                    this.printSuccessResponse(request, fileContent.get());
                } else {
                    this.printNotFoundResponse(request);
                }
            }
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
