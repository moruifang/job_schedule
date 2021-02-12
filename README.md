# 代码库说明

在我们实际应用场景中，特别是数据处理场景中（比如数据质量分析），经常需要执行互相独立的多个任务，而这多个任务分解的子任务又较相似，但任务拓扑不同，就需要任务编排功能将子任务串起来，按照配置的依赖关系执行。

本代码库实现了一个轻量级的任务调度框架，提供任务拓扑分析功能，将同一优先级不同任务的相同子任务合并批量计算，用户只需要关心计算的业务逻辑，可以是本地代码，也可以是远程服务、数据库等。并提供了根据子任务运行情况，再执行任务拓扑子图的功能。

框架调用分为三层：

1. 同一依赖层级的任务合并在一起，level
2. 同一依赖层级下同一task_key的任务合并在一起，task_key
3. 同一task_key下相同业务参数的合并在一起，group

设计如图所示

<img src="/Users/moruifang/my_github/job_schedule/img/frame.png" alt="frame" style="zoom: 50%;" />

# 实现demo

### demo数据

file1: uid, date, val1,  val2

file2: uid, timestamp, val3 

### 任务说明

task1: 计算平均值

task2: 将时间戳转成日期类型

task3: 按照条件过滤数据

task4: 合并上游数据

task5: 计算最大值

### 任务运行

执行 ./run.py，实际使用时，只需修改run.py中user edit注释部分，注册自己的实现类即可

# 配置说明

可根据自己的业务需要，添加需要的配置项，但框架需要一些配置保障正常运行，以下为框架所需的配置项。

为了区别是否常量，使用双引号来区分，有双引号的为常量，没有双引号的为用户需要设置的值。

```json
{job_id1 : {"inputs":"***,***", 
            "output":"***", 
            "tasks":{task_key1:{"impl_name":"***","upstreams":[***,***], "inputs":"***,***", "ignore_job_inputs":true，"tasks":{子任务拓扑，同上层tasks}}, 
                     task_key2:{...}
                    }
           },
 job_id2 : {...}
}
```

+ job_id（必选，唯一）:	用户为任务设置的唯一标志，string类型
+ job下inputs（可选）：整个job的全局输入，每个task都将该输入作为输入，除非设置了ignore_job_inputs标志
+ job下output（可选）：整个job的输出
+ job下tasks (必选）：job的子任务拓扑，承载了整个job的子任务
+ task_key（必选，唯一）：通常使用实现名作为task_key，若多个子任务使用同一实现，则需保证唯一
+ impl_name（可选）：执行该task的实现类，如果taks_key不为实现名，则必填
+ upstreams（可选）：填写依赖的上游task_key，若无依赖则可不填
+ inputs（可选）：该子任务的输入
+ ignore_job_inputs（可选）：若job下配置了全局inputs，但该子任务不需要使用，则设置为true
+ task下tasks（可选）：如果需要根据该父任务决定是否执行子拓扑图，则设置，配置方法同上层tasks

# 代码API

见代码doc