package dev.vality.file.storage.servlet;

import dev.vality.file.storage.FileStoragePresignedMultipartSrv;
import dev.vality.woody.thrift.impl.http.THServiceBuilder;
import jakarta.servlet.*;
import jakarta.servlet.annotation.WebServlet;
import lombok.RequiredArgsConstructor;

import java.io.IOException;

@WebServlet("/file_storage/presigned-multipart/v1")
@RequiredArgsConstructor
public class FileStoragePresignedMultipartServlet extends GenericServlet {

    private final FileStoragePresignedMultipartSrv.Iface fileStoragePresignedMultipartHandler;

    private Servlet thriftServlet;

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        thriftServlet = new THServiceBuilder()
                .build(FileStoragePresignedMultipartSrv.Iface.class, fileStoragePresignedMultipartHandler);
    }

    @Override
    public void service(ServletRequest req, ServletResponse res) throws ServletException, IOException {
        thriftServlet.service(req, res);
    }
}
