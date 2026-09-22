-- A process-local pump can be replaced while retaining the same durable
-- active_stream row (for example, a DVR reservation outlives a viewer pump).
-- Generation ownership prevents an old pump's delayed OnExit CAS from
-- terminalizing the replacement generation.
ALTER TABLE active_stream
    ADD COLUMN pump_generation uuid;
