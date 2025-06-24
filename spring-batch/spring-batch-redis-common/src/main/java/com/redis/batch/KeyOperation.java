package com.redis.batch;

/**
 * <pre>
 * From Debezium:
 * ╔═══════════╦════════════════════════════════════════════╗
 * ║ Operation ║                Description                 ║
 * ╠═══════════╬════════════════════════════════════════════╣
 * ║ Create    ║ A new row was inserted                     ║
 * ║ Update    ║ An existing row was modified               ║
 * ║ Delete    ║ A row was deleted                          ║
 * ║ Read      ║ A row was read (applies only to snapshots) ║
 * ╚═══════════╩════════════════════════════════════════════╝
 * </pre>
 */
public enum KeyOperation {
    CREATE, READ, UPDATE, DELETE
}
