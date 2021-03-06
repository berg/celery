import smtplib
import warnings

try:
    from email.mime.text import MIMEText
except ImportError:
    from email.MIMEText import MIMEText


class SendmailWarning(UserWarning):
    """Problem happened while sending the e-mail message."""


class Message(object):

    def __init__(self, to=None, sender=None, subject=None, body=None,
            charset="us-ascii"):
        self.to = to
        self.sender = sender
        self.subject = subject
        self.body = body
        self.charset = charset

        if not isinstance(self.to, (list, tuple)):
            self.to = [self.to]

    def __repr__(self):
        return "<E-mail: To:%r Subject:%r>" % (self.to, self.subject)

    def __str__(self):
        msg = MIMEText(self.body, "plain", self.charset)
        msg["Subject"] = self.subject
        msg["From"] = self.sender
        msg["To"] = ", ".join(self.to)
        return msg.as_string()


class Mailer(object):

    def __init__(self, host="localhost", port=0, user=None, password=None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password

    def send(self, message, fail_silently=False):
        try:
            client = smtplib.SMTP(self.host, self.port)

            if self.user and self.password:
                client.login(self.user, self.password)

            client.sendmail(message.sender, message.to, str(message))
            client.quit()
        except Exception, exc:
            if not fail_silently:
                raise
            warnings.warn(SendmailWarning(
                "E-mail could not be sent: %r %r" % (exc, message)))
