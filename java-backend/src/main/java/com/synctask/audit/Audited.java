package com.synctask.audit;

import com.synctask.entity.AuditLog;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标记一个 controller 方法需要写审计日志。
 *
 * <p><b>为什么改成注解 + 切面</b>：原来每个 controller 自己调 {@code auditLogService}，
 * 结果 96 个端点里 58 个一处审计都没有，而且漏掉的恰恰是碰数据最深的那几类——
 * 证书私钥上传删除（4 个端点）、元数据探查（13 个）、逐行内容对比（6 个）、
 * 高级功能（35 个）全部为 0。手写的覆盖率靠人自觉，守不住；
 * 注解至少让"这个端点要不要审计"变成代码评审时看得见的一行。
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Audited {

    /** 记什么动作。 */
    AuditLog.Action value();

    /**
     * 从哪个参数取业务对象 id（工作流/任务 id）。
     * <p>取方法参数的下标，-1 表示这个动作没有关联对象。
     */
    int idArg() default -1;

    /**
     * 是否把方法入参一并记进 details。
     * <p><b>默认 false</b>：入参里可能有连接串、口令、证书 PEM，
     * 把它们写进审计表等于换个地方明文存凭证。只有确认参数无敏感值时才打开。
     */
    boolean recordArgs() default false;
}
