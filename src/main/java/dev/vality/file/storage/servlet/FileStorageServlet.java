package dev.vality.file.storage.servlet;

import dev.vality.file.storage.FileStorageSrv;
import dev.vality.woody.thrift.impl.http.THServiceBuilder;
import lombok.RequiredArgsConstructor;

import javax.servlet.*;
import javax.servlet.annotation.WebServlet;
import java.io.IOException;

@WebServlet("/file_storage")
@RequiredArgsConstructor
public class FileStorageServlet extends GenericServlet {

    private final FileStorageSrv.Iface fileStorageHandler;

    private Servlet thriftServlet;

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        thriftServlet = new THServiceBuilder()
                .build(FileStorageSrv.Iface.class, fileStorageHandler);
    }

    @Override
    public void service(ServletRequest req, ServletResponse res) throws ServletException, IOException {
        thriftServlet.service(req, res);
    }
}
