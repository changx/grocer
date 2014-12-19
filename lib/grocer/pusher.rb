module Grocer
  class Pusher
    def initialize(connection, options={})
      @connection = connection
      @previous_notifications = Grocer::History.new(size: options[:history_size])
      @resend_on_not_found = options[:resend_on_not_found] || false
    end

    def push(notification)
      remember_notification(notification)
      push_out(notification)
      notification.mark_sent
    end

    def push_and_retry(notifications, errors=[])
      Array(notifications).each do |notification|
        push(notification)
        check_and_retry(errors)
      end
      errors
    end

    def read_error(timeout=0)
      if response = @connection.read_with_timeout(Grocer::ErrorResponse::LENGTH, timeout)
        @connection.close
        Grocer::ErrorResponse.new(response)
      end
    end

    def read_error_and_history(timeout=0)
      if response = read_error(timeout)
        if response.false_alarm?
          clear_notifications
        else
          @previous_notifications.find_culpret(response)
        end
      end
      response
    end

    def check_and_retry(errors=[], timeout=0)
      if (response = read_error_and_history(timeout)) && ! response.false_alarm?
        errors << response
        push_and_retry(response.resend, errors) if response.notification || resend_on_not_found
      end
      errors
    end

    def remembered_notifications?
      !@previous_notifications.empty?
    end

    def clear_notifications
      @previous_notifications.clear
    end

    def inspect
      "#<Pusher>"
    end

    private

    def push_out(notification)
      @connection.write(notification.to_bytes)
    end

    def remember_notification(notification)
      @previous_notifications.remember(notification)
    end

    #def initialize(connection)
      #@connection = connection
    #end

    #def push(notification)
      #@connection.write(notification.to_bytes)
    #end
  end
end
