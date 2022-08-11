package cool.xiaokang.ETL.interceptor;

import cool.xiaokang.ETL.utils.JSONUtil;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

public class ETLInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    /**
     * 校验json字符串是否完整
     *
     * @param event
     * @return event对象
     */
    @Override
    public Event intercept(Event event) {
        //1 获取json数据
        byte[] body = event.getBody();
        String log = new String(body, StandardCharsets.UTF_8);

        //2 校验json数据
        if (JSONUtil.isJSONValidate(log)) {
            return event;
        } else {
            return null;
        }
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        System.out.println("批量处理");
        Iterator<Event> it = list.iterator();
        while (it.hasNext()) {
            Event event = it.next();
            if (intercept(event) == null) {
                it.remove();
            }
        }
        System.out.println("批量结束");
        return list;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new ETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
