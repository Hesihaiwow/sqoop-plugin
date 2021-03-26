package plugin;

import plugin.decimal.ImportDecimalTool;
import com.cloudera.sqoop.tool.ToolDesc;
import org.apache.sqoop.tool.ToolPlugin;

import java.util.Collections;
import java.util.List;

/**
 * TODO
 *
 * @author hsh
 * @create 2020年10月19日
 */
public class SqoopPlugins extends ToolPlugin {
    @Override
    public List<ToolDesc> getTools() {
        return Collections
                .singletonList(new ToolDesc("import-decimal", ImportDecimalTool.class, "Mysql上的decimal数据导入到HDFS"));
    }
}