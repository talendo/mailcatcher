require "active_support/json"
require "eventmachine"
require "mail"
require "pg"

module MailCatcher::Mail extend self
  def db
    @__db ||= begin
      db = PG.connect(ENV['DATABASE_URL'])
      db.exec('DROP TABLE message_part')
      db.exec('DROP TABLE message')
      db.exec(<<-SQL)
          CREATE TABLE message (
            id INTEGER PRIMARY KEY,
            sender TEXT,
            recipients TEXT,
            subject TEXT,
            source TEXT,
            size TEXT,
            type TEXT,
            created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
          )
      SQL

      db.exec(<<-SQL)
          CREATE SEQUENCE message_id_seq
          START WITH 1
          INCREMENT BY 1
          NO MINVALUE
          NO MAXVALUE
          CACHE 1;
      SQL

      db.exec(<<-SQL)
          ALTER SEQUENCE message_id_seq OWNED BY message.id;
      SQL

      db.exec(<<-SQL)
          ALTER TABLE ONLY message ALTER COLUMN id SET DEFAULT nextval('message_id_seq'::regclass);
      SQL

      db.exec(<<-SQL)
          CREATE TABLE message_part (
            id INTEGER PRIMARY KEY,
            message_id character varying NOT NULL,
            cid TEXT,
            type TEXT,
            is_attachment INTEGER,
            filename TEXT,
            charset TEXT,
            body TEXT,
            size INTEGER,
            created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
          )
      SQL

      db.exec(<<-SQL)
          CREATE SEQUENCE message_part_id_seq
          START WITH 1
          INCREMENT BY 1
          NO MINVALUE
          NO MAXVALUE
          CACHE 1;
      SQL

      db.exec(<<-SQL)
          ALTER SEQUENCE message_part_id_seq OWNED BY message_part.id;
      SQL

      db.exec(<<-SQL)
          ALTER TABLE ONLY message_part ALTER COLUMN id SET DEFAULT nextval('message_part_id_seq'::regclass);
      SQL

      db
    end
  end

  def add_message(message)
    @add_message_query ||= db.prepare('statement1', "INSERT INTO message (sender, recipients, subject, source, type, size, created_at) VALUES ($1, $2, $3, $4, $5, $6, NOW()) RETURNING id")

    mail = Mail.new(message[:source])
    res = db.exec_prepared('statement1', [message[:sender], message[:recipients].to_json, mail.subject, message[:source], mail.mime_type || "text/plain", message[:source].length])
    message_id = res.getvalue(0, 0).to_i

    parts = mail.all_parts
    parts = [mail] if parts.empty?
    parts.each do |part|
      body = part.body.to_s
      # Only parts have CIDs, not mail
      cid = part.cid if part.respond_to? :cid
      add_message_part(message_id, cid, part.mime_type || "text/plain", part.attachment? ? 1 : 0, part.filename, part.charset, body, body.length)
    end

    EventMachine.next_tick do
      message = MailCatcher::Mail.message message_id
      MailCatcher::Events::MessageAdded.push message
    end
  end

  def add_message_part(*args)
    @add_message_part_query ||= db.prepare('statement2', "INSERT INTO message_part (message_id, cid, type, is_attachment, filename, charset, body, size, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())")
    db.exec_prepared('statement2', args)
  end

  def latest_created_at
    @latest_created_at_query ||= db.prepare('statement3', "SELECT created_at FROM message ORDER BY created_at DESC LIMIT 1")
    db.exec_prepared('statement3', []).getvalue(0,'created_at')
  end

  def messages
    @messages_query ||= db.prepare('statement4', "SELECT id, sender, recipients, subject, size, created_at FROM message ORDER BY created_at, id ASC")
    res = db.exec_prepared('statement4', [])
    res.values.map do |row|
      Hash[res.fields.zip(row)].tap do |message|
        message["recipients"] &&= ActiveSupport::JSON.decode message["recipients"]
      end
    end
  end

  def message(id)
    @message_query ||= db.prepare('statement5', "SELECT * FROM message WHERE id = $1 LIMIT 1")
    res = db.exec_prepared('statement5', [id])
    raw = res.values.first
    raw && Hash[res.fields.zip(raw)].tap do |message|
      message["recipients"] &&= ActiveSupport::JSON.decode message["recipients"]
    end
  end

  def message_has_html?(id)
    @message_has_html_query ||= db.prepare('statement6', "SELECT 1 FROM message_part WHERE message_id = $1 AND is_attachment = 0 AND type IN ('application/xhtml+xml', 'text/html') LIMIT 1")
    (!!db.exec_prepared('statement6', [id]).getvalue(0, 0)) || ["text/html", "application/xhtml+xml"].include?(message(id)["type"])
  end

  def message_has_plain?(id)
    @message_has_plain_query ||= db.prepare('statement7', "SELECT 1 FROM message_part WHERE message_id = $1 AND is_attachment = 0 AND type = 'text/plain' LIMIT 1")
    (!!db.exec_prepared('statement7', [id]).getvalue(0, 0)) || message(id)["type"] == "text/plain"
  end

  def message_parts(id)
    @message_parts_query ||= db.prepare('statement8', "SELECT cid, type, filename, size FROM message_part WHERE message_id = $1 ORDER BY filename ASC")
    res = db.exec_prepared('statement8', [id])
    res.values.map do |row|
      Hash[res.fields.zip(row)]
    end
  end

  def message_attachments(id)
    @message_attachments_query ||= db.prepare('statement9', "SELECT cid, type, filename, size FROM message_part WHERE message_id = $1 AND is_attachment = 1 ORDER BY filename ASC")
    res = db.exec_prepared('statement9', [id])
    res.values.map do |row|
      Hash[res.fields.zip(row)]
    end
  end

  def message_part(message_id, part_id)
    @message_part_query ||= db.prepare('statement10', "SELECT * FROM message_part WHERE message_id = $1 AND id = $2 LIMIT 1")
    res = db.exec_prepared('statement10', [message_id, part_id])
    row = res.values.first
    row && Hash[res.fields.zip(row)]
  end

  def message_part_type(message_id, part_type)
    @message_part_type_query ||= db.prepare('statement11', "SELECT * FROM message_part WHERE message_id = $1 AND type = $2 AND is_attachment = 0 LIMIT 1")
    res = db.exec_prepared('statement11', [message_id, part_type])
    row = res.values.first
    row && Hash[res.fields.zip(row)]
  end

  def message_part_html(message_id)
    part = message_part_type(message_id, "text/html")
    part ||= message_part_type(message_id, "application/xhtml+xml")
    part ||= begin
      message = message(message_id)
      message if message.present? and ["text/html", "application/xhtml+xml"].include? message["type"]
    end
  end

  def message_part_plain(message_id)
    message_part_type message_id, "text/plain"
  end

  def message_part_cid(message_id, cid)
    @message_part_cid_query ||= db.prepare('statement12', "SELECT * FROM message_part WHERE message_id = $1")
    res = db.exec_prepared('statement12', [message_id])
    res.values.map do |row|
      Hash[res.fields.zip(row)]
    end.find do |part|
      part["cid"] == cid
    end
  end

  def delete!
    @delete_messages_query ||= db.prepare('statement13', "DELETE FROM message")
    @delete_message_parts_query ||= db.prepare('statement14', "DELETE FROM message_part")

    db.exec_prepared('statement13', []) and
    db.exec_prepared('statement14', [])
  end

  def delete_message!(message_id)
    @delete_messages_query ||= db.prepare('statement15', "DELETE FROM message WHERE id = $1")
    @delete_message_parts_query ||= db.prepare('statement16', "DELETE FROM message_part WHERE message_id = $1")
    db.exec_prepared('statement15', [message_id]) and
    db.exec_prepared('statement16', [message_id])
  end
end
