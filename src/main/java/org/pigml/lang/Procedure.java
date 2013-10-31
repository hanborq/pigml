package org.pigml.lang;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 9/18/13
 * Time: 10:50 AM
 * To change this template use File | Settings | File Templates.
 */
public interface Procedure<C> {
    void execute(C context) throws ProcedureEndException;

    static public class ProcedureEndException extends RuntimeException {}

    static public final Procedure NOP = new Procedure() {
        @Override
        public void execute(Object context) throws ProcedureEndException {
        }
    };
}
