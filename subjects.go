package transport

import "fmt"

// SubjectAnnounce is where a plugin process publishes its Announcement at boot.
const SubjectAnnounce = "plugins.announce"

// SessionEvents is the per-session async event subject (used by Backend later).
func SessionEvents(session string) string {
	return fmt.Sprintf("session.%s.events", session)
}
